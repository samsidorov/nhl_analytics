
-- Validation and Analysis Queries for NHL Analytics Pipeline

-- 1. Raw payload counts by source
SELECT source, COUNT(*) AS payload_count
FROM nhl.raw_api_payloads
GROUP BY source
ORDER BY payload_count DESC;

-- 2. Total row counts for each layer
SELECT 'raw' AS layer, COUNT(*) AS row_count FROM nhl.raw_api_payloads
UNION ALL
SELECT 'teams', COUNT(*) FROM nhl.dim_teams
UNION ALL
SELECT 'games', COUNT(*) FROM nhl.dim_games
UNION ALL
SELECT 'fact_team_game_stats', COUNT(*) FROM nhl.fact_team_game_stats;

-- 3. Latest ingestion timestamp per source
SELECT source, MAX(created_at) AS last_ingested_at
FROM nhl.raw_api_payloads
GROUP BY source
ORDER BY last_ingested_at DESC;

-- 4. Check for duplicate team rows by team_id
SELECT team_id, COUNT(*)
FROM nhl.dim_teams
GROUP BY team_id
HAVING COUNT(*) > 1;

-- 5. Check for duplicate game rows by game_id
SELECT game_id, COUNT(*)
FROM nhl.dim_games
GROUP BY game_id
HAVING COUNT(*) > 1;

-- 6. Check for duplicate fact rows by game_id/team_id
SELECT game_id, team_id, COUNT(*)
FROM nhl.fact_team_game_stats
GROUP BY game_id, team_id
HAVING COUNT(*) > 1;

-- 7. Orphan fact records where game or team does not exist
SELECT f.game_id, f.team_id
FROM nhl.fact_team_game_stats f
LEFT JOIN nhl.dim_games g ON f.game_id = g.game_id
LEFT JOIN nhl.dim_teams t ON f.team_id = t.team_id
WHERE g.game_id IS NULL OR t.team_id IS NULL;

-- 8. Fact rows without matching season or date metadata
SELECT f.game_id, f.team_id, f.season, f.game_date
FROM nhl.fact_team_game_stats f
LEFT JOIN nhl.dim_games g ON f.game_id = g.game_id
WHERE g.game_id IS NULL OR f.season IS NULL OR f.game_date IS NULL
ORDER BY f.game_id, f.team_id
LIMIT 100;

-- 9. Recent game metadata with team names
SELECT g.game_id,
       g.season,
       g.game_date,
       ht.team_name AS home_team,
       at.team_name AS away_team,
       g.game_status
FROM nhl.dim_games g
LEFT JOIN nhl.dim_teams ht ON g.home_team_id = ht.team_id
LEFT JOIN nhl.dim_teams at ON g.away_team_id = at.team_id
ORDER BY g.game_date DESC
LIMIT 50;

-- 10. Recent fact rows with team and game context
SELECT f.game_id,
       g.game_date,
       t.team_name,
       f.goals,
       f.shots,
       f.hits,
       f.power_play_goals
FROM nhl.fact_team_game_stats f
LEFT JOIN nhl.dim_games g ON f.game_id = g.game_id
LEFT JOIN nhl.dim_teams t ON f.team_id = t.team_id
ORDER BY g.game_date DESC, f.team_id
LIMIT 100;

-- 11. Aggregate team-level season metrics
SELECT t.team_name,
       f.season,
       COUNT(*) AS games_played,
       SUM(f.goals) AS total_goals,
       SUM(f.shots) AS total_shots,
       SUM(f.hits) AS total_hits,
       SUM(f.power_play_goals) AS total_power_play_goals,
       AVG(f.goals) AS avg_goals_per_game,
       AVG(f.shots) AS avg_shots_per_game
FROM nhl.fact_team_game_stats f
JOIN nhl.dim_teams t ON f.team_id = t.team_id
GROUP BY t.team_name, f.season
ORDER BY f.season DESC, total_goals DESC
LIMIT 50;

-- 12. Top teams by average goals per game in the latest season
WITH latest_season AS (
    SELECT MAX(season) AS season FROM nhl.fact_team_game_stats
)
SELECT t.team_name,
       f.season,
       AVG(f.goals) AS avg_goals_per_game,
       COUNT(*) AS games_played
FROM nhl.fact_team_game_stats f
JOIN nhl.dim_teams t ON f.team_id = t.team_id
JOIN latest_season ls ON f.season = ls.season
GROUP BY t.team_name, f.season
HAVING COUNT(*) >= 1
ORDER BY avg_goals_per_game DESC
LIMIT 20;

-- 13. Game-level summary for a given game_id
-- Replace :game_id with the specific game identifier
SELECT f.game_id,
       g.game_date,
       t.team_name,
       CASE WHEN f.team_id = g.home_team_id THEN 'home' ELSE 'away' END AS home_away,
       f.goals,
       f.shots,
       f.hits,
       f.power_play_goals
FROM nhl.fact_team_game_stats f
JOIN nhl.dim_games g ON f.game_id = g.game_id
JOIN nhl.dim_teams t ON f.team_id = t.team_id
WHERE f.game_id = :game_id
ORDER BY home_away;

-- 14. Payload counts for schedule versus boxscore ingestion
SELECT source,
       COUNT(*) AS payload_count,
       MIN(created_at) AS first_ingest,
       MAX(created_at) AS last_ingest
FROM nhl.raw_api_payloads
GROUP BY source
ORDER BY source;
