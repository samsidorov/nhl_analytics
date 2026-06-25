# NHL Analytics Pipeline — Requirements Document

## 1. Overview

The NHL Analytics Pipeline is a data engineering project designed to ingest, process, and model NHL game data using a layered architecture. The system uses **Apache Airflow**, **PostgreSQL**, and NHL public APIs to build an analytics-ready data warehouse with a **raw → dimension → fact** structure.

The goal is to support scalable hockey analytics such as team performance tracking, game statistics analysis, and historical trend reporting.

---

# 2. Architecture

## 2.1 Data Flow

```
NHL Public API      ↓[ RAW LAYER ]nhl.raw_api_payloads      ↓[ DIMENSION LAYER ]nhl.dim_teamsnhl.dim_games      ↓[ FACT LAYER ]nhl.fact_team_game_stats
```

---

# 3. Data Layers

---

# 3.1 RAW LAYER (Ingestion)

## Table: `nhl.raw_api_payloads`

### Purpose

Stores **unaltered API responses** for reproducibility, debugging, and reprocessing.

### Requirements

- Must store raw JSON exactly as received
- Must not apply transformations
- Must support multiple API sources
- Must include metadata for traceability

### Schema

```
CREATE TABLE nhl.raw_api_payloads (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Data Sources

- NHL schedule API
- NHL boxscore API
- future endpoints (players, standings, etc.)

---

## Responsibilities (Airflow DAG: `nhl_ingestion_dag`)

- Fetch data from NHL API
- Store raw JSON in PostgreSQL
- Ensure idempotent ingestion where possible
- No transformations allowed

---

# 3.2 DIMENSION LAYER

## Purpose

Transforms raw NHL data into **structured analytical entities**.

---

## 3.2.1 Dimension: Teams

### Table: `nhl.dim_teams`

### Purpose

Stores NHL team reference data.

### Schema

```
CREATE TABLE nhl.dim_teams (
    team_id INT PRIMARY KEY,
    team_name TEXT,
    abbreviation TEXT,
    conference TEXT,
    division TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Requirements

- One row per team
- Must be deduplicated by `team_id`
- Must be updatable (UPSERT logic)

---

## 3.2.2 Dimension: Games

### Table: `nhl.dim_games`

### Purpose

Stores structured game metadata.

### Schema

```
CREATE TABLE nhl.dim_games (
    game_id INT PRIMARY KEY,
    season INT,
    game_date DATE,
    home_team_id INT,
    away_team_id INT,
    game_status TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Requirements

- One row per game
- Must reference teams logically via `team_id`
- Must support updates (game status changes)

---

## Responsibilities (Airflow DAG: `nhl_dimensions_dag`)

- Read from `raw_api_payloads`
- Extract teams and games
- Normalize JSON structure
- Perform UPSERT into dimension tables
- Ensure no duplicate entities

---

# 3.3 FACT LAYER

## Purpose

Stores **analytics-ready performance metrics at the grain of team per game**.

---

## Table: `nhl.fact_team_game_stats`

### Schema

```
CREATE TABLE nhl.fact_team_game_stats (
    id SERIAL PRIMARY KEY,
    game_id INT NOT NULL,
    team_id INT NOT NULL,
    goals INT,
    shots INT,
    hits INT,
    power_play_goals INT,
    season INT,
    game_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (game_id, team_id)
);
```

---

## Grain Definition

> One record per **team per game**

Example:

- Game 1 → Home Team row
- Game 1 → Away Team row

---

## Requirements

- Must join with `dim_games` and `dim_teams`
- Must be idempotent (UPSERT by `game_id + team_id`)
- Must support reprocessing safely
- Must only contain numeric/aggregated stats

---

## Responsibilities (Airflow DAG: `nhl_fact_team_game_stats_dag`)

- Read from dimension tables
- Fetch or reuse structured game stats
- Flatten boxscore data
- Map teams to dimension IDs
- Insert/update fact table

---

# 4. Airflow DAG Architecture

---

## 4.1 DAG: `nhl_ingestion_dag`

### Purpose

Raw ingestion layer

### Tasks

- Fetch NHL API data
- Store JSON in `raw_api_payloads`

---

## 4.2 DAG: `nhl_dimensions_dag`

### Purpose

Build dimension tables

### Tasks

- Extract teams → `dim_teams`
- Extract games → `dim_games`

---

## 4.3 DAG: `nhl_fact_team_game_stats_dag`

### Purpose

Build analytics layer

### Tasks

- Read `dim_games`
- Fetch/parse stats
- Join dimensions
- Load fact table

---

# 5. Data Quality Requirements

## Must enforce:

- No duplicate primary keys
- No orphan fact records (must match dims)
- Null-safe transformations for API fields
- Stable schema for downstream analytics

---

# 6. System Constraints

- Local Docker-based PostgreSQL
- Apache Airflow 2.8+
- LocalExecutor
- NHL public API (no authentication required)
- Idempotent DAG execution required

---

# 7. Non-Functional Requirements

## Reliability

- DAGs must be retry-safe
- API failures must not corrupt data

## Maintainability

- Clear separation of ingestion, transformation, analytics

## Observability

- Logs available in Airflow UI
- Raw payloads stored for debugging

---

# 8. Future Enhancements (Roadmap)

## Phase 2

- Player-level fact table
- Season aggregates
- Advanced metrics (Corsi, expected goals)

## Phase 3

- dbt migration for transformations
- Snowflake / BigQuery migration
- BI dashboards (Power BI / Tableau)

## Phase 4

- Real-time ingestion (event-driven pipelines)
- Caching NHL API responses
- Data quality tests (Great Expectations)

---

# 🧠 Final Summary

This project implements a **modern data warehouse pattern**:

- 🔵 Raw layer → preserves source truth
- 🟡 Dimension layer → structured entities
- 🟢 Fact layer → analytics-ready metrics

It demonstrates:

- ETL/ELT pipeline design
- Airflow orchestration
- Star schema modeling
- API-based ingestion
- Production-style data engineering principles
