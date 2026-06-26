from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from nhl_utils import ensure_nhl_schema, fetch_json, fetch_stats_json, get_missing_boxscore_games, insert_raw_payload

HISTORY_DAYS = 300

with DAG(
    dag_id="nhl_historical_backfill_dag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nhl", "backfill"],
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

    @task(task_id="fetch_nhl_teams")
    def fetch_teams():
        team_payload = fetch_stats_json("/team", params={"limit": -1})
        source = f"team_list:{datetime.utcnow().date().isoformat()}"
        insert_raw_payload(source, team_payload)
        return "team-list-stored"

    @task(task_id="fetch_historical_scores")
    def fetch_historical_scores():
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=HISTORY_DAYS - 1)
        score_payloads = []

        for offset in range((end_date - start_date).days + 1):
            target_date = start_date + timedelta(days=offset)
            date_str = target_date.isoformat()
            payload = fetch_json(f"/v1/score/{date_str}")
            insert_raw_payload(f"score:{date_str}", payload)
            score_payloads.append(payload)

        return score_payloads

    @task(task_id="fetch_missing_boxscores")
    def fetch_missing_boxscores():
        missing_games = get_missing_boxscore_games()
        for game_id, game_date in missing_games:
            boxscore_payload = fetch_json(f"/v1/gamecenter/{game_id}/boxscore")
            insert_raw_payload(f"boxscore:{game_id}:{game_date}", boxscore_payload)
        return f"stored-{len(missing_games)}-boxscores"

    score_payloads = fetch_historical_scores()
    missing_boxscores = fetch_missing_boxscores()

    start >> ensure_raw_layer() >> [fetch_teams(), score_payloads]
    score_payloads >> missing_boxscores >> complete
