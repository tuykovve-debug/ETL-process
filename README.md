DAG `flatten_pets` скачивает JSON, преобразует в CSV и сохраняет в `data/pets_flat.csv`.

* `cd airflow-pets`
* `docker compose up -d`
* Открыть http://localhost:8080 (login/pass – `airflow` / `airflow`)
* В UI найти DAG `flatten_pets` → включить → Trigger DAG
* После выполнения результат будет в файле `data/pets_flat.csv` на хосте.
