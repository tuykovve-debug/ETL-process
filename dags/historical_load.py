from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
CLEAN_PATH = "/opt/airflow/data/clean_iot_temp.parquet"
HIST_OUTPUT = "/opt/airflow/data/output/historical_load.parquet"
INCR_OUTPUT = "/opt/airflow/data/output/incremental_load.parquet"
N_DAYS = 3
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

    os.makedirs(os.path.dirname(CLEAN_PATH), exist_ok=True)
    df_clean.to_parquet(CLEAN_PATH, index=False)


def load_historical():
    df_clean = pd.read_parquet(CLEAN_PATH)
    os.makedirs(os.path.dirname(HIST_OUTPUT), exist_ok=True)
    df_clean.to_parquet(HIST_OUTPUT, index=False)


def load_incremental(execution_date, **context):
    ref_date = datetime.strptime(execution_date, "%Y-%m-%d").date()

    df_clean = pd.read_parquet(CLEAN_PATH)

    start_date = ref_date - timedelta(days=N_DAYS)
    mask = (df_clean["noted_date"] >= start_date) & (df_clean["noted_date"] <= ref_date)
    incr = df_clean[mask]

    os.makedirs(os.path.dirname(INCR_OUTPUT), exist_ok=True)
    incr.to_parquet(INCR_OUTPUT, index=False)


with DAG(
        dag_id="etl_iot_temp",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["etl", "iot", "incremental"],
) as dag:

    transform = PythonOperator(
        task_id="process_temperature",
        python_callable=process_temperature,
    )

    historical_load = PythonOperator(
        task_id="load_historical",
        python_callable=load_historical,
    )

    incremental_load = PythonOperator(
        task_id="load_incremental",
        python_callable=load_incremental,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    transform >> [historical_load, incremental_load]
