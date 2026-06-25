from datetime import datetime
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ----------------------------
# CONFIG
# ----------------------------
BASE_URL = "https://api-web.nhle.com/v1"


# ----------------------------
# FETCH NHL DATA
# ----------------------------
def fetch_nhl_data(**context):
    endpoint = "/standings/now"
    url = BASE_URL + endpoint

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    # push to XCom
    return {
        "endpoint": endpoint,
        "payload": data
    }


# ----------------------------
# STORE INTO POSTGRES
# ----------------------------
def load_to_postgres(**context):
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="fetch_nhl_data")

    endpoint = result["endpoint"]
    payload = result["payload"]

    pg = PostgresHook(postgres_conn_id="postgres_default")

    # 1. insert ingestion run
    run_id = pg.get_first("""
        INSERT INTO nhl.ingestion_runs (dag_id, source, endpoint, status)
        VALUES (%s, %s, %s, %s)
        RETURNING run_id
    """, parameters=("nhl_ingestion_dag", "nhl_api", endpoint, "running"))[0]

    # 2. insert raw payload
    pg.run("""
        INSERT INTO nhl.raw_api_payloads (run_id, endpoint, payload)
        VALUES (%s, %s, %s)
    """, parameters=(run_id, endpoint, json.dumps(payload)))

    # 3. mark success
    pg.run("""
        UPDATE nhl.ingestion_runs
        SET status = 'success',
            finished_at = NOW(),
            records_fetched = %s
        WHERE run_id = %s
    """, parameters=(len(payload.get("standings", [])), run_id))


# ----------------------------
# DAG DEFINITION
# ----------------------------
default_args = {
    "owner": "sam",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="nhl_ingestion_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    fetch = PythonOperator(
        task_id="fetch_nhl_data",
        python_callable=fetch_nhl_data
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    fetch >> load