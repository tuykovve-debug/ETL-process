from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import csv
import os
import xml.etree.ElementTree as ET

BASE_PATH = "/opt/airflow"
DATA_DIR = os.path.join(BASE_PATH, "data")

NUTRITION_XML = os.path.join(DATA_DIR, "nutrition.xml")
NUTRITION_CSV = os.path.join(DATA_DIR, "nutrition_flat.csv")

def convert_xml_to_csv():
    print(f"Reading XML from: {NUTRITION_XML}")
    print(f"Writing CSV to:   {NUTRITION_CSV}")

    tree = ET.parse(NUTRITION_XML)
    root = tree.getroot()

    rows = []
    for food in root.findall("food"):
        serving_el = food.find("serving")
        calories_el = food.find("calories")
        vitamins_el = food.find("vitamins")
        minerals_el = food.find("minerals")

        row = {
            "name": food.findtext("name"),
            "mfr": food.findtext("mfr"),
            "serving": serving_el.text if serving_el is not None else None,
            "serving_units": serving_el.get("units") if serving_el is not None else None,
            "calories_total": calories_el.get("total") if calories_el is not None else None,
            "calories_fat": calories_el.get("fat") if calories_el is not None else None,
            "total_fat": food.findtext("total-fat"),
            "saturated_fat": food.findtext("saturated-fat"),
            "cholesterol": food.findtext("cholesterol"),
            "sodium": food.findtext("sodium"),
            "carb": food.findtext("carb"),
            "fiber": food.findtext("fiber"),
            "protein": food.findtext("protein"),
            "vit_a": vitamins_el.findtext("a") if vitamins_el is not None else None,
            "vit_c": vitamins_el.findtext("c") if vitamins_el is not None else None,
            "min_ca": minerals_el.findtext("ca") if minerals_el is not None else None,
            "min_fe": minerals_el.findtext("fe") if minerals_el is not None else None,
        }
        rows.append(row)

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(NUTRITION_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {NUTRITION_CSV}")

with DAG(
        dag_id="nutrition_xml_to_csv",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    convert_xml_to_csv_task = PythonOperator(
        task_id="convert_xml_to_csv",
        python_callable=convert_xml_to_csv,
    )
