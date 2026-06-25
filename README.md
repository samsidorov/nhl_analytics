# nhl_analytics
NHL Analytics Platform
Open-source NHL analytics platform that ingests historical and real-time hockey data from public APIs, builds analytics-ready datasets, and powers an AI agent for natural language sports research, insights, and predictions.

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
