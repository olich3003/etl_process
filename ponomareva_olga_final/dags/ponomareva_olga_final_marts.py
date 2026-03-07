from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN = "postgres_default"

@dag(dag_id="ponomareva_olga_final_marts", start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def main():
    @task
    def build_dau():
        pg = PostgresHook(postgres_conn_id=PG_CONN)
        pg.run("""
        INSERT INTO mart_dau(date, dau)
        SELECT start_time::date AS date, COUNT(DISTINCT user_id) AS dau
        FROM stg_user_sessions
        GROUP BY 1
        ON CONFLICT (date) DO UPDATE SET dau = EXCLUDED.dau;
        """)

    @task
    def build_support():
        pg = PostgresHook(postgres_conn_id=PG_CONN)
        pg.run("""
        INSERT INTO mart_support_sla(date, tickets_created, tickets_closed, avg_resolution_hours)
        SELECT
          created_at::date AS date,
          COUNT(*) AS tickets_created,
          SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS tickets_closed,
          AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0) AS avg_resolution_hours
        FROM stg_support_tickets
        GROUP BY 1
        ON CONFLICT (date) DO UPDATE SET
          tickets_created = EXCLUDED.tickets_created,
          tickets_closed = EXCLUDED.tickets_closed,
          avg_resolution_hours = EXCLUDED.avg_resolution_hours;
        """)
    build_dau()
    build_support()

main()
