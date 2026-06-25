from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from nhl_utils import (
    ensure_nhl_schema,
    extract_games_from_schedule,
    extract_teams_from_schedule,
    upsert_games,
    upsert_teams,
)

with DAG(
    dag_id="nhl_dimensions_dag",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nhl", "dimensions"],
) as dag:

    start = EmptyOperator(task_id="start")
    complete = EmptyOperator(task_id="complete")

    @task(task_id="ensure_dimension_tables")
    def ensure_dimension_layer():
        ensure_nhl_schema()
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "CREATE TABLE IF NOT EXISTS nhl.dim_teams ("
            "team_id INT PRIMARY KEY, "
            "team_name TEXT, "
            "abbreviation TEXT, "
            "conference TEXT, "
            "division TEXT, "
            "updated_at TIMESTAMP DEFAULT NOW()"
            ");"
        )
        hook.run(
            "CREATE TABLE IF NOT EXISTS nhl.dim_games ("
            "game_id INT PRIMARY KEY, "
            "season INT, "
            "game_date DATE, "
            "home_team_id INT, "
            "away_team_id INT, "
            "game_status TEXT, "
            "updated_at TIMESTAMP DEFAULT NOW()"
            ");"
        )
        return None

    @task(task_id="extract_schedule_payload")
    def extract_schedule_payload():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        row = hook.get_first(
            "SELECT payload FROM nhl.raw_api_payloads WHERE source = %s ORDER BY created_at DESC LIMIT 1;",
            parameters=("schedule",),
        )
        return row[0] if row else {}

    @task(task_id="upsert_teams")
    def load_teams(schedule_payload):
        teams = extract_teams_from_schedule(schedule_payload)
        upsert_teams(teams)
        return len(teams)

    @task(task_id="upsert_games")
    def load_games(schedule_payload):
        games = extract_games_from_schedule(schedule_payload)
        upsert_games(games)
        return len(games)

    schedule_payload = extract_schedule_payload()

    start >> ensure_dimension_layer() >> schedule_payload
    schedule_payload >> [load_teams(schedule_payload), load_games(schedule_payload)] >> complete
