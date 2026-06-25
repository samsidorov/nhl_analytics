from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from nhl_utils import (
    ensure_nhl_schema,
    fetch_game_stats,
    get_all_game_ids,
    get_game_metadata,
    upsert_fact_rows,
)

with DAG(
    dag_id="nhl_fact_team_game_stats_dag",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nhl", "fact"],
) as dag:

    start = EmptyOperator(task_id="start")
    complete = EmptyOperator(task_id="complete")

    @task(task_id="ensure_fact_table")
    def ensure_fact_layer():
        ensure_nhl_schema()
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "CREATE TABLE IF NOT EXISTS nhl.fact_team_game_stats ("
            "id SERIAL PRIMARY KEY, "
            "game_id INT NOT NULL, "
            "team_id INT NOT NULL, "
            "goals INT, "
            "shots INT, "
            "hits INT, "
            "power_play_goals INT, "
            "season INT, "
            "game_date DATE, "
            "created_at TIMESTAMP DEFAULT NOW(), "
            "UNIQUE (game_id, team_id)"
            ");"
        )
        return None

    @task(task_id="build_fact_rows")
    def build_facts():
        game_ids = get_all_game_ids()
        rows = []
        for game_id in game_ids:
            metadata = get_game_metadata(game_id)
            stats = fetch_game_stats(game_id)
            for team_stats in stats:
                if team_stats["team_id"] is None:
                    continue
                rows.append(
                    {
                        "game_id": game_id,
                        "team_id": team_stats["team_id"],
                        "goals": team_stats["goals"],
                        "shots": team_stats["shots"],
                        "hits": team_stats["hits"],
                        "power_play_goals": team_stats["power_play_goals"],
                        "season": metadata.get("season"),
                        "game_date": metadata.get("game_date"),
                    }
                )
        upsert_fact_rows(rows)
        return len(rows)

    start >> ensure_fact_layer() >> build_facts() >> complete
