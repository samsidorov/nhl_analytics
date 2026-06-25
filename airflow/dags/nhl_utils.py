import json
from datetime import datetime
from typing import Any, Dict, Iterable, List

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = "https://statsapi.web.nhl.com/api/v1"


def get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_default")


def fetch_json(endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
    url = f"{API_BASE}{endpoint}"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def ensure_nhl_schema() -> None:
    hook = get_postgres_hook()
    hook.run("CREATE SCHEMA IF NOT EXISTS nhl;")


def payload_exists(source: str, payload: Dict[str, Any]) -> bool:
    hook = get_postgres_hook()
    result = hook.get_first(
        "SELECT 1 FROM nhl.raw_api_payloads WHERE source = %s AND payload = %s LIMIT 1;",
        parameters=(source, json.dumps(payload)),
    )
    return result is not None


def insert_raw_payload(source: str, payload: Dict[str, Any]) -> None:
    hook = get_postgres_hook()
    if payload_exists(source, payload):
        return
    hook.run(
        "INSERT INTO nhl.raw_api_payloads (source, payload, created_at) VALUES (%s, %s, NOW());",
        parameters=(source, json.dumps(payload)),
    )


def extract_teams_from_schedule(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    teams: Dict[int, Dict[str, Any]] = {}
    for date in payload.get("dates", []):
        for game in date.get("games", []):
            for side in ("home", "away"):
                team = game["teams"][side]["team"]
                conference = team.get("conference", {}).get("name")
                division = team.get("division", {}).get("name")
                teams[team["id"]] = {
                    "team_id": team["id"],
                    "team_name": team.get("name"),
                    "abbreviation": team.get("abbreviation"),
                    "conference": conference,
                    "division": division,
                }
    return list(teams.values())


def extract_games_from_schedule(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    games = []
    for date in payload.get("dates", []):
        game_date = date.get("date")
        for game in date.get("games", []):
            games.append(
                {
                    "game_id": game["gamePk"],
                    "season": int(game.get("season", 0)),
                    "game_date": game_date,
                    "home_team_id": game["teams"]["home"]["team"]["id"],
                    "away_team_id": game["teams"]["away"]["team"]["id"],
                    "game_status": game.get("status", {}).get("detailedState"),
                }
            )
    return games


def upsert_teams(teams: Iterable[Dict[str, Any]]) -> None:
    hook = get_postgres_hook()
    sql = (
        "INSERT INTO nhl.dim_teams (team_id, team_name, abbreviation, conference, division, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, NOW()) "
        "ON CONFLICT (team_id) DO UPDATE SET "
        "team_name = EXCLUDED.team_name, "
        "abbreviation = EXCLUDED.abbreviation, "
        "conference = EXCLUDED.conference, "
        "division = EXCLUDED.division, "
        "updated_at = NOW();"
    )
    for team in teams:
        hook.run(sql, parameters=(
            team["team_id"],
            team.get("team_name"),
            team.get("abbreviation"),
            team.get("conference"),
            team.get("division"),
        ))


def upsert_games(games: Iterable[Dict[str, Any]]) -> None:
    hook = get_postgres_hook()
    sql = (
        "INSERT INTO nhl.dim_games (game_id, season, game_date, home_team_id, away_team_id, game_status, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, NOW()) "
        "ON CONFLICT (game_id) DO UPDATE SET "
        "season = EXCLUDED.season, "
        "game_date = EXCLUDED.game_date, "
        "home_team_id = EXCLUDED.home_team_id, "
        "away_team_id = EXCLUDED.away_team_id, "
        "game_status = EXCLUDED.game_status, "
        "updated_at = NOW();"
    )
    for game in games:
        hook.run(sql, parameters=(
            game["game_id"],
            game.get("season"),
            game.get("game_date"),
            game.get("home_team_id"),
            game.get("away_team_id"),
            game.get("game_status"),
        ))


def fetch_game_stats(game_id: int) -> List[Dict[str, Any]]:
    payload = fetch_json(f"/game/{game_id}/boxscore")
    stats = []
    teams = payload.get("teams", {})
    for side in ("home", "away"):
        team_data = teams.get(side, {})
        team = team_data.get("team", {})
        team_stats = team_data.get("teamStats", {}).get("teamSkaterStats", {})
        stats.append(
            {
                "game_id": game_id,
                "team_id": team.get("id"),
                "goals": int(team_stats.get("goals", 0) or 0),
                "shots": int(team_stats.get("shots", 0) or 0),
                "hits": int(team_stats.get("hits", 0) or 0),
                "power_play_goals": int(team_stats.get("powerPlayGoals", 0) or 0),
            }
        )
    return stats


def upsert_fact_rows(rows: Iterable[Dict[str, Any]]) -> None:
    hook = get_postgres_hook()
    sql = (
        "INSERT INTO nhl.fact_team_game_stats (game_id, team_id, goals, shots, hits, power_play_goals, season, game_date, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) "
        "ON CONFLICT (game_id, team_id) DO UPDATE SET "
        "goals = EXCLUDED.goals, "
        "shots = EXCLUDED.shots, "
        "hits = EXCLUDED.hits, "
        "power_play_goals = EXCLUDED.power_play_goals, "
        "season = EXCLUDED.season, "
        "game_date = EXCLUDED.game_date, "
        "created_at = NOW();"
    )
    for row in rows:
        hook.run(sql, parameters=(
            row["game_id"],
            row["team_id"],
            row.get("goals"),
            row.get("shots"),
            row.get("hits"),
            row.get("power_play_goals"),
            row.get("season"),
            row.get("game_date"),
        ))


def get_all_game_ids() -> List[int]:
    hook = get_postgres_hook()
    rows = hook.get_records("SELECT game_id FROM nhl.dim_games;")
    return [row[0] for row in rows]


def get_game_metadata(game_id: int) -> Dict[str, Any]:
    hook = get_postgres_hook()
    row = hook.get_first(
        "SELECT season, game_date FROM nhl.dim_games WHERE game_id = %s;",
        parameters=(game_id,),
    )
    return {"season": row[0], "game_date": row[1]} if row else {}
