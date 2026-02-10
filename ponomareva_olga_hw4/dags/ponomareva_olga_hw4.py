from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CLEAN_CSV = "/opt/airflow/data/processed/clean.csv"
PG_CONN = "postgres_default"
TABLE = "iot_temp_clean"

DAYS_BACK = 3


def ensure_table(hook):
    hook.run(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id          TEXT,
        room_id     TEXT,
        noted_date  DATE,
        temp        NUMERIC,
        out_in      TEXT
    );
    """)


@dag(dag_id="ponomareva_olga_hw4_full", start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def full_load():
    @task
    def run():
        hook = PostgresHook(postgres_conn_id=PG_CONN)
        ensure_table(hook)

        df = pd.read_csv(CLEAN_CSV)
        df = df.rename(columns={"room_id/id": "room_id", "out/in": "out_in"})
        df["id"] = df["id"].astype(str)
        df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date

        hook.run(f"TRUNCATE TABLE {TABLE};")

        rows = df[["id", "room_id", "noted_date", "temp", "out_in"]].values.tolist()
        hook.insert_rows(TABLE, rows, target_fields=["id", "room_id", "noted_date", "temp", "out_in"])

    run()


@dag(dag_id="ponomareva_olga_hw4_inc", start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def inc_load():
    @task
    def run():
        hook = PostgresHook(postgres_conn_id=PG_CONN)
        ensure_table(hook)

        df = pd.read_csv(CLEAN_CSV)
        df = df.rename(columns={"room_id/id": "room_id", "out/in": "out_in"})
        df["id"] = df["id"].astype(str)
        df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date

        cutoff = datetime.today().date() - timedelta(days=DAYS_BACK)
        df = df[df["noted_date"] >= cutoff].copy()

        hook.run(f"DELETE FROM {TABLE} WHERE noted_date >= %s;", parameters=(cutoff,))

        if len(df) > 0:
            rows = df[["id", "room_id", "noted_date", "temp", "out_in"]].values.tolist()
            hook.insert_rows(TABLE, rows, target_fields=["id", "room_id", "noted_date", "temp", "out_in"])

    run()


full_load()
inc_load()
