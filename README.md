# nhl_analytics
NHL Analytics Platform

Open-source NHL analytics platform that ingests historical and real-time hockey data from public APIs, builds analytics-ready datasets, and powers analytics workflows.

## Local Airflow setup

This repository uses Docker Compose to run PostgreSQL and Airflow.

- Copy or update `docker/.env` from `docker/.env.example`
- Set `AIRFLOW__WEBSERVER__SECRET_KEY` to the same value for all Airflow components
- Run `docker compose up` from the `docker` directory

Example environment variables in `docker/.env`:

```env
POSTGRES_USER=nhl_user
POSTGRES_PASSWORD=nhl_pass
POSTGRES_DB=nhl_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
AIRFLOW_UID=50000
AIRFLOW__WEBSERVER__SECRET_KEY=super_secret_key_please_change
```

> Important: use a strong secret key in production and ensure service clocks are synchronized across machines.

## Pipeline overview

This project contains the following Airflow DAGs:

- `nhl_ingestion_dag`
  - Daily ingestion of raw NHL payloads
  - Fetches the latest team list and daily score snapshots
  - Detects and backfills missing score dates when the DAG has not run for several days
  - Fetches missing boxscore payloads for all ingested games
- `nhl_historical_backfill_dag`
  - One-time backfill for a historical window
  - Currently configured to ingest the last 60 days of `/v1/score/YYYY-MM-DD` payloads
  - Stores daily raw score JSON and missing boxscore JSON
- `nhl_dimensions_dag`
  - Builds dimension tables from raw payload snapshots
  - Loads latest team metadata and all stored score snapshots
- `nhl_fact_team_game_stats_dag`
  - Builds team-level fact rows from boxscore payloads
  - Aggregates goals, shots, hits, and power-play goals from player boxscore data

## Data model

The pipeline stores raw JSON and builds analytics tables in the `nhl` schema:

- `nhl.raw_api_payloads`
  - Stores raw JSON payloads by `source`
  - Uses prefixed sources such as `team_list:YYYY-MM-DD`, `score:YYYY-MM-DD`, and `boxscore:<game_id>:<date>`
- `nhl.dim_teams`
  - Team dimension data loaded from the latest team list snapshot
- `nhl.dim_games`
  - Game dimension data loaded from stored score snapshots
- `nhl.fact_team_game_stats`
  - Team-level fact rows built from boxscore payloads
  - Includes `goals`, `shots`, `hits`, and `power_play_goals`

## Backfill guidance

To backfill historic data:

1. Trigger `nhl_historical_backfill_dag` manually in Airflow.
2. Confirm raw score dates were ingested by querying `nhl.raw_api_payloads` for `source LIKE 'score:%'`.
3. Re-run `nhl_dimensions_dag` and `nhl_fact_team_game_stats_dag` after the raw backfill completes.

> Note: Some date ranges may contain zero games if the API returns no scheduled contests for that window.

## Validation and analysis

Use `sql/validation_and_analysis.sql` for validation queries, including:

- raw payload counts by source
- row counts for each layer
- ingestion timestamps
- duplicate and orphan checks
- recent game and fact preview queries

## Notes

- No persistent database schema changes are required outside the tables created by the DAGs.
- The project uses documented NHL API endpoints and stores each ingestion snapshot with a unique source key for auditability.
