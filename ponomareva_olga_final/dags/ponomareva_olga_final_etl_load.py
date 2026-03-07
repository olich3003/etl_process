from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient
from psycopg2.extras import execute_values


PG_CONN = "postgres_default"

MONGO_HOST = "mongo"
MONGO_USER = "mongo"
MONGO_PASS = "mongo"
MONGO_DB = "etl_final"


def chunks(a, n=5000):
    for i in range(0, len(a), n):
        yield a[i:i+n]


@dag(dag_id="ponomareva_olga_final_etl_load", start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def main():
    @task
    def run():
        # --- connect ---
        client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/")
        db = client[MONGO_DB]

        pg = PostgresHook(postgres_conn_id=PG_CONN)
        conn = pg.get_conn()
        cur = conn.cursor()

        # --- full reload (самый простой и без дублей) ---
        cur.execute("TRUNCATE stg_session_pages;")
        cur.execute("TRUNCATE stg_session_actions;")
        cur.execute("TRUNCATE stg_ticket_messages;")
        cur.execute("TRUNCATE stg_recommended_products;")
        cur.execute("TRUNCATE stg_review_flags;")

        cur.execute("TRUNCATE stg_user_sessions;")
        cur.execute("TRUNCATE stg_event_logs;")
        cur.execute("TRUNCATE stg_support_tickets;")
        cur.execute("TRUNCATE stg_user_recommendations;")
        cur.execute("TRUNCATE stg_moderation_queue;")
        conn.commit()

        # --- 1) UserSessions -> 3 tables ---
        docs = list(db.user_sessions.find({}, {"_id": 0}))
        sess_rows = []
        page_rows = []
        act_rows = []

        for d in docs:
            dev = d.get("device") or {}
            sess_rows.append((
                d["session_id"],
                d["user_id"],
                d["start_time"],
                d["end_time"],
                dev.get("type"),
                dev.get("os"),
                dev.get("browser"),
            ))
            for p in (d.get("pages_visited") or []):
                page_rows.append((d["session_id"], p))
            for a in (d.get("actions") or []):
                act_rows.append((d["session_id"], a))

        execute_values(
            cur,
            "INSERT INTO stg_user_sessions(session_id,user_id,start_time,end_time,device_type,device_os,device_browser) VALUES %s",
            sess_rows,
            page_size=5000
        )
        if page_rows:
            execute_values(cur, "INSERT INTO stg_session_pages(session_id,page) VALUES %s ON CONFLICT DO NOTHING", page_rows, page_size=5000)
        if act_rows:
            execute_values(cur, "INSERT INTO stg_session_actions(session_id,action) VALUES %s ON CONFLICT DO NOTHING", act_rows, page_size=5000)
        conn.commit()

        # --- 2) EventLogs -> stg_event_logs (details раскладываем по колонкам) ---
        docs = list(db.event_logs.find({}, {"_id": 0}))
        ev_rows = []
        for d in docs:
            det = d.get("details") or {}
            ev_rows.append((
                d["event_id"],
                d["timestamp"],
                d["event_type"],
                det.get("user_id"),
                det.get("page"),
                det.get("product_id"),
                det.get("amount"),
                det.get("element"),
            ))

        for part in chunks(ev_rows, 5000):
            execute_values(
                cur,
                "INSERT INTO stg_event_logs(event_id,ts,event_type,user_id,page,product_id,amount,element) VALUES %s",
                part,
                page_size=5000
            )
        conn.commit()

        # --- 3) SupportTickets -> 2 tables ---
        docs = list(db.support_tickets.find({}, {"_id": 0}))
        t_rows = []
        m_rows = []
        for d in docs:
            t_rows.append((
                d["ticket_id"],
                d["user_id"],
                d["status"],
                d["issue_type"],
                d["created_at"],
                d["updated_at"],
            ))
            for m in (d.get("messages") or []):
                m_rows.append((
                    d["ticket_id"],
                    m.get("sender"),
                    m.get("message"),
                    m.get("timestamp"),
                ))

        execute_values(
            cur,
            "INSERT INTO stg_support_tickets(ticket_id,user_id,status,issue_type,created_at,updated_at) VALUES %s",
            t_rows,
            page_size=5000
        )
        if m_rows:
            execute_values(cur, "INSERT INTO stg_ticket_messages(ticket_id,sender,message,ts) VALUES %s ON CONFLICT DO NOTHING", m_rows, page_size=5000)
        conn.commit()

        # --- 4) UserRecommendations -> 2 tables ---
        docs = list(db.user_recommendations.find({}, {"_id": 0}))
        r_rows = []
        rp_rows = []
        for d in docs:
            r_rows.append((d["user_id"], d["last_updated"]))
            for pid in (d.get("recommended_products") or []):
                rp_rows.append((d["user_id"], pid))

        execute_values(
            cur,
            "INSERT INTO stg_user_recommendations(user_id,last_updated) VALUES %s",
            r_rows,
            page_size=5000
        )
        if rp_rows:
            execute_values(cur, "INSERT INTO stg_recommended_products(user_id,product_id) VALUES %s ON CONFLICT DO NOTHING", rp_rows, page_size=5000)
        conn.commit()

        # --- 5) ModerationQueue -> 2 tables ---
        docs = list(db.moderation_queue.find({}, {"_id": 0}))
        q_rows = []
        f_rows = []
        for d in docs:
            q_rows.append((
                d["review_id"],
                d["user_id"],
                d["product_id"],
                d["review_text"],
                d["rating"],
                d["moderation_status"],
                d["submitted_at"],
            ))
            for fl in (d.get("flags") or []):
                f_rows.append((d["review_id"], fl))

        execute_values(
            cur,
            "INSERT INTO stg_moderation_queue(review_id,user_id,product_id,review_text,rating,moderation_status,submitted_at) VALUES %s",
            q_rows,
            page_size=5000
        )
        if f_rows:
            execute_values(cur, "INSERT INTO stg_review_flags(review_id,flag) VALUES %s ON CONFLICT DO NOTHING", f_rows, page_size=5000)
        conn.commit()

        cur.close()
        conn.close()
        client.close()

    run()


main()
