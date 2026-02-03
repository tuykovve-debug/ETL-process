from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"

def process_temperature():
    df = pd.read_csv(DATA_PATH)
    df = df[df["out/in"] == "In"]

    df["noted_date"] = pd.to_datetime(
        df["noted_date"],
        format="%d-%m-%Y %H:%M",
        errors="coerce"
    ).dt.date

    p05 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df_clean = df[(df["temp"] >= p05) & (df["temp"] <= p95)]

    daily = df_clean.groupby("noted_date", as_index=False)["temp"].mean()
    hot5 = daily.nlargest(5, "temp")
    cold5 = daily.nsmallest(5, "temp")

    out_dir = "/opt/airflow/data/output"
    os.makedirs(out_dir, exist_ok=True)

    hot5.to_csv(os.path.join(out_dir, "hot5.csv"), index=False)
    cold5.to_csv(os.path.join(out_dir, "cold5.csv"), index=False)

    print("Данные после фильтрации и преобразования даты:")
    print(df_clean.head())

    print("5 самых жарких дней:")
    print(hot5)

    print("5 самых холодых дней:")
    print(cold5)

with DAG(
        dag_id="etl",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["etl", "csv"],
) as dag:
    run_etl = PythonOperator(
        task_id="process_temperature",
        python_callable=process_temperature,
    )