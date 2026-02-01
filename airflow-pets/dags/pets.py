from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import json
import csv
import os
import requests

PETS_URL = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
BASE_PATH = "/opt/airflow"
DATA_DIR = os.path.join(BASE_PATH, "data")

SRC_PATH = os.path.join(DATA_DIR, "pets-data.json")
DST_PATH = os.path.join(DATA_DIR, "pets_flat.csv")

def download_and_flatten():
    os.makedirs(DATA_DIR, exist_ok=True)

    resp = requests.get(PETS_URL, timeout=10)
    resp.raise_for_status()
    with open(SRC_PATH, "w", encoding="utf-8") as f:
        f.write(resp.text)

    with open(SRC_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)["pets"]

    rows = []
    for pet in data:
        favs = pet.get("favFoods", [])
        rows.append(
            {
                "name": pet.get("name"),
                "species": pet.get("species"),
                "birthYear": pet.get("birthYear"),
                "photo": pet.get("photo"),
                "favFood1": favs[0] if len(favs) > 0 else None,
                "favFood2": favs[1] if len(favs) > 1 else None,
                "favFood3": favs[2] if len(favs) > 2 else None,
            }
        )

    with open(DST_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "name",
                "species",
                "birthYear",
                "photo",
                "favFood1",
                "favFood2",
                "favFood3",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


with DAG(
        dag_id="flatten_pets",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="download_and_flatten",
        python_callable=download_and_flatten,
    )
