-- RAW LAYER
CREATE TABLE nhl.raw_api_payloads (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- DIMENSION LAYER
CREATE TABLE nhl.dim_teams (
    team_id INT PRIMARY KEY,
    team_name TEXT,
    abbreviation TEXT,
    conference TEXT,
    division TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE nhl.dim_games (
    game_id INT PRIMARY KEY,
    season INT,
    game_date DATE,
    home_team_id INT,
    away_team_id INT,
    game_status TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- FACT LAYER
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
