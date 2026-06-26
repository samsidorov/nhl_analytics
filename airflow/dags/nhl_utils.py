import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = "https://api-web.nhle.com"
STATS_API_BASE = "https://api.nhle.com/stats/rest/en"


def get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_default")


def fetch_json(endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
    url = f"{API_BASE}{endpoint}"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_stats_json(endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
    url = f"{STATS_API_BASE}{endpoint}"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def ensure_nhl_schema() -> None:
    hook = get_postgres_hook()
    hook.run("CREATE SCHEMA IF NOT EXISTS nhl;")


def payload_exists(source: str) -> bool:
    hook = get_postgres_hook()
    result = hook.get_first(
        "SELECT 1 FROM nhl.raw_api_payloads WHERE source = %s LIMIT 1;",
        parameters=(source,),
    )
    return result is not None


def insert_raw_payload(source: str, payload: Dict[str, Any]) -> None:
    hook = get_postgres_hook()
    if payload_exists(source):
        return
    hook.run(
        "INSERT INTO nhl.raw_api_payloads (source, payload, created_at) VALUES (%s, %s, NOW());",
        parameters=(source, json.dumps(payload)),
    )


def get_missing_boxscore_games() -> List[Tuple[int, str]]:
    hook = get_postgres_hook()
    score_rows = hook.get_records(
        "SELECT source, payload FROM nhl.raw_api_payloads WHERE source LIKE %s;",
        parameters=("score:%",),
    )
    boxscore_rows = hook.get_records(
        "SELECT source FROM nhl.raw_api_payloads WHERE source LIKE %s;",
        parameters=("boxscore:%",),
    )
    existing_boxscores = {row[0] for row in boxscore_rows}
    missing_games: List[Tuple[int, str]] = []

    for source, payload in score_rows:
        _, date_str = source.split(":", 1)
        for game in payload.get("games", []):
            game_id = game.get("id")
            if not game_id:
                continue
            boxscore_source = f"boxscore:{game_id}:{date_str}"
            if boxscore_source not in existing_boxscores:
                missing_games.append((game_id, date_str))

    return missing_games


def extract_teams_from_team_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    teams = []
    for team in payload.get("data", []):
        teams.append(
            {
                "team_id": team.get("id"),
                "team_name": team.get("fullName"),
                "abbreviation": team.get("triCode"),
                "conference": None,
                "division": None,
            }
        )
    return teams


def extract_teams_from_schedule(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    teams: Dict[int, Dict[str, Any]] = {}
    for week in payload.get("gameWeek", []):
        for game in week.get("games", []):
            for side in ("homeTeam", "awayTeam"):
                team = game.get(side, {})
                if not team:
                    continue
                teams[team["id"]] = {
                    "team_id": team["id"],
                    "team_name": team.get("commonName", {}).get("default") if isinstance(team.get("commonName"), dict) else team.get("commonName"),
                    "abbreviation": team.get("abbrev"),
                    "conference": None,
                    "division": None,
                }
    return list(teams.values())


def extract_games_from_score_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    games = []
    for game in payload.get("games", []):
        home_team = game.get("homeTeam", {})
        away_team = game.get("awayTeam", {})
        games.append(
            {
                "game_id": game.get("id"),
                "season": int(game.get("season", 0)),
                "game_date": game.get("gameDate"),
                "home_team_id": home_team.get("id"),
                "away_team_id": away_team.get("id"),
                "game_status": game.get("gameState") or game.get("gameScheduleState"),
            }
        )
    return games


def extract_games_from_schedule(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    games = []
    for week in payload.get("gameWeek", []):
        game_date = week.get("date")
        for game in week.get("games", []):
            home_team = game.get("homeTeam", {})
            away_team = game.get("awayTeam", {})
            games.append(
                {
                    "game_id": game["id"],
                    "season": int(game.get("season", 0)),
                    "game_date": game_date,
                    "home_team_id": home_team.get("id"),
                    "away_team_id": away_team.get("id"),
                    "game_status": game.get("gameState") or game.get("gameScheduleState"),
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
            team.get("team_id"),
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


def _sum_team_boxscore_stats(team_boxscore: Dict[str, Any]) -> Tuple[int, int]:
    hits = 0
    power_play_goals = 0
    for role in ("forwards", "defense"):
        for player in team_boxscore.get(role, []):
            hits += int(player.get("hits") or 0)
            power_play_goals += int(player.get("powerPlayGoals") or 0)
    return hits, power_play_goals


def fetch_game_stats(game_id: int) -> List[Dict[str, Any]]:
    payload = fetch_json(f"/v1/gamecenter/{game_id}/boxscore")
    stats = []
    player_stats = payload.get("playerByGameStats", {})
    for side in ("homeTeam", "awayTeam"):
        team = payload.get(side, {})
        team_player_stats = player_stats.get(side, {})
        hits, power_play_goals = _sum_team_boxscore_stats(team_player_stats)
        stats.append(
            {
                "game_id": game_id,
                "team_id": team.get("id"),
                "goals": int(team.get("score", 0) or 0),
                "shots": int(team.get("sog", 0) or 0),
                "hits": hits,
                "power_play_goals": power_play_goals,
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
