-- =========================================================
-- NHL Analytics Schema (Airflow + NHL separation)
-- Schema strategy:
--   public  -> Airflow metadata (DO NOT TOUCH)
--   nhl     -> Your analytics data
-- docker exec -i nhl_postgres psql -U nhl_user -d nhl_db < docker/sql/schema.sql
-- =========================================================

-- -----------------------------
-- 0. CREATE NHL SCHEMA
-- -----------------------------

CREATE SCHEMA IF NOT EXISTS nhl;

-- =========================================================
-- 1. INGESTION / OBSERVABILITY LAYER
-- =========================================================

CREATE TABLE IF NOT EXISTS nhl.ingestion_runs (
    run_id SERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    source TEXT NOT NULL,              -- e.g. "nhl_api"
    endpoint TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT NOW(),
    finished_at TIMESTAMP,
    status TEXT,                       -- success / failed
    records_fetched INT DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS nhl.raw_api_payloads (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES nhl.ingestion_runs(run_id),
    endpoint TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT NOW(),
    payload JSONB NOT NULL
);

-- =========================================================
-- 2. CORE DIMENSIONS
-- =========================================================

CREATE TABLE IF NOT EXISTS nhl.teams (
    team_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    abbreviation TEXT,
    city TEXT,
    conference TEXT,
    division TEXT,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS nhl.players (
    player_id INT PRIMARY KEY,
    full_name TEXT NOT NULL,
    position TEXT,
    team_id INT REFERENCES nhl.teams(team_id),
    birth_date DATE,
    nationality TEXT,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS nhl.games (
    game_id INT PRIMARY KEY,
    season INT,
    game_date DATE,
    home_team_id INT REFERENCES nhl.teams(team_id),
    away_team_id INT REFERENCES nhl.teams(team_id),
    home_score INT,
    away_score INT,
    status TEXT,                      -- scheduled / final / live
    venue TEXT
);

-- =========================================================
-- 3. FACT TABLES (ANALYTICS LAYER)
-- =========================================================

CREATE TABLE IF NOT EXISTS nhl.player_game_stats (
    id SERIAL PRIMARY KEY,
    game_id INT REFERENCES nhl.games(game_id),
    player_id INT REFERENCES nhl.players(player_id),
    team_id INT REFERENCES nhl.teams(team_id),

    goals INT DEFAULT 0,
    assists INT DEFAULT 0,
    shots INT DEFAULT 0,
    plus_minus INT DEFAULT 0,
    penalty_minutes INT DEFAULT 0,
    time_on_ice_seconds INT DEFAULT 0,

    UNIQUE (game_id, player_id)
);

CREATE TABLE IF NOT EXISTS nhl.team_game_stats (
    id SERIAL PRIMARY KEY,
    game_id INT REFERENCES nhl.games(game_id),
    team_id INT REFERENCES nhl.teams(team_id),

    goals INT DEFAULT 0,
    shots INT DEFAULT 0,
    hits INT DEFAULT 0,
    power_play_goals INT DEFAULT 0,
    penalty_kill_success BOOLEAN,

    UNIQUE (game_id, team_id)
);

-- =========================================================
-- 4. OPTIONAL DIMENSIONS
-- =========================================================

CREATE TABLE IF NOT EXISTS nhl.seasons (
    season INT PRIMARY KEY,
    start_year INT,
    end_year INT
);

-- =========================================================
-- 5. STANDINGS SNAPSHOT (ANALYTICS HISTORY)
-- =========================================================

CREATE TABLE IF NOT EXISTS nhl.standings_snapshot (
    id SERIAL PRIMARY KEY,
    season INT,
    team_id INT REFERENCES nhl.teams(team_id),
    snapshot_date DATE DEFAULT CURRENT_DATE,

    wins INT DEFAULT 0,
    losses INT DEFAULT 0,
    ot_losses INT DEFAULT 0,
    points INT DEFAULT 0
);