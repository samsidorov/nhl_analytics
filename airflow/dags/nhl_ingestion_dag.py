from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from nhl_utils import ensure_nhl_schema, fetch_json, insert_raw_payload

with DAG(
    dag_id="nhl_ingestion_dag",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nhl", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")
    complete = EmptyOperator(task_id="complete")

    @task(task_id="ensure_raw_schema")
    def ensure_raw_layer():
        ensure_nhl_schema()
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "CREATE TABLE IF NOT EXISTS nhl.raw_api_payloads ("
            "id SERIAL PRIMARY KEY, "
            "source TEXT NOT NULL, "
            "payload JSONB NOT NULL, "
            "created_at TIMESTAMP DEFAULT NOW()"
            ");"
        )
        return None

    @task(task_id="fetch_nhl_schedule")
    def fetch_schedule():
        schedule_payload = fetch_json("/schedule")
        insert_raw_payload("schedule", schedule_payload)
        return "schedule-stored"

    @task(task_id="fetch_nhl_boxscores")
    def fetch_boxscores():
        schedule_payload = fetch_json("/schedule")
        games = []
        for date in schedule_payload.get("dates", []):
            for game in date.get("games", []):
                games.append(game["gamePk"])

        for game_id in games:
            boxscore_payload = fetch_json(f"/game/{game_id}/boxscore")
            insert_raw_payload(f"boxscore:{game_id}", boxscore_payload)
        return f"stored-{len(games)}-boxscores"

    start >> ensure_raw_layer() >> fetch_schedule() >> fetch_boxscores() >> complete
