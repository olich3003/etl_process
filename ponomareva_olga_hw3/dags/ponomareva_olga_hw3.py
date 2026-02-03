import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task


RAW = "/opt/airflow/data/raw"
OUT = "/opt/airflow/data/processed"
RES = "/opt/airflow/data/results"

INP = f"{RAW}/IOT-temp.csv"


@dag(dag_id="ponomareva_olga_hw3", start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def main():
    @task
    def run():
        os.makedirs(OUT, exist_ok=True)
        os.makedirs(RES, exist_ok=True)

        df = pd.read_csv(INP)

        # №1 5 самых жарких и холодных дней 
        df["temp"] = pd.to_numeric(df["temp"], errors="raise")
        dt = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M", errors="raise")
        df["date"] = dt.dt.strftime("%Y-%m-%d")

        daily = df.groupby("date", as_index=False)["temp"].mean()
        daily.sort_values("temp", ascending=False).head(5).to_csv(f"{RES}/hottest_5.csv", index=False)
        daily.sort_values("temp", ascending=True).head(5).to_csv(f"{RES}/coldest_5.csv", index=False)

        # №2 out/in == in
        df["out/in"] = df["out/in"].astype(str).str.strip().str.lower()
        df = df[df["out/in"] == "in"].copy()

        # №3 yyyy-MM-dd
        dt2 = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M", errors="raise")
        df["noted_date"] = dt2.dt.strftime("%Y-%m-%d")

        # №4 очистка по 5 и 95 процентилям 
        p5 = df["temp"].quantile(0.05)
        p95 = df["temp"].quantile(0.95)
        df = df[(df["temp"] >= p5) & (df["temp"] <= p95)].copy()

        cols = ["id", "room_id/id", "noted_date", "temp", "out/in"]
        df[cols].to_csv(f"{OUT}/clean.csv", index=False)

    run()


main()
