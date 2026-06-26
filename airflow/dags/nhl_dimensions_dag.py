from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from nhl_utils import (
    ensure_nhl_schema,
    extract_games_from_score_payload,
    extract_teams_from_team_payload,
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

    @task(task_id="extract_team_payload")
    def extract_team_payload():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        row = hook.get_first(
            "SELECT payload FROM nhl.raw_api_payloads WHERE source LIKE %s ORDER BY source DESC LIMIT 1;",
            parameters=("team_list:%",),
        )
        return row[0] if row else {}

    @task(task_id="extract_score_payloads")
    def extract_score_payloads():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = hook.get_records(
            "SELECT payload FROM nhl.raw_api_payloads WHERE source LIKE %s ORDER BY source;",
            parameters=("score:%",),
        )
        return [row[0] for row in rows]

    @task(task_id="upsert_teams")
    def load_teams(team_payload):
        teams = extract_teams_from_team_payload(team_payload)
        upsert_teams(teams)
        return len(teams)

    @task(task_id="upsert_games")
    def load_games(score_payloads):
        games = []
        for score_payload in score_payloads:
            games.extend(extract_games_from_score_payload(score_payload))
        upsert_games(games)
        return len(games)

    team_payload = extract_team_payload()
    score_payloads = extract_score_payloads()

    start >> ensure_dimension_layer() >> [team_payload, score_payloads]
    team_payload >> load_teams(team_payload)
    score_payloads >> load_games(score_payloads)
    [load_teams(team_payload), load_games(score_payloads)] >> complete
