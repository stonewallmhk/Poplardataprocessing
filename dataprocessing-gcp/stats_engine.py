"""
stats_engine.py
Incremental stats computation engine used by 04_sync.py.

Called after every new match is written to Firestore:
  1. update_season_stats() - adds this match's contribution to each
     player's season_stats/{year} document
  2. update_career_stats() - full recalculation of career_stats/summary
     for each affected player from all their season_stats documents

Design principle: career stats always use full recalculation (not incremental)
to ensure accuracy and avoid cumulative rounding errors across seasons.
"""

import logging
from google.cloud import firestore
import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_int(value, default: int = 0) -> int:
    try:
        return int(value) if value not in (None, "", "null") else default
    except (ValueError, TypeError):
        return default


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "", "null") else default
    except (ValueError, TypeError):
        return default


def overs_to_balls(overs: float) -> int:
    """8.4 overs = 8 * 6 + 4 = 52 balls."""
    whole = int(overs)
    partial = round((overs - whole) * 10)
    return whole * 6 + partial


def balls_to_overs(balls: int) -> float:
    """52 balls = 8.4 overs."""
    return float(f"{balls // 6}.{balls % 6}")


def safe_divide(num: float, den: float) -> float:
    return round(num / den, 2) if den else 0.0


def is_better_bowling(new_w: int, new_r: int, best_w: int, best_r: int) -> bool:
    if new_w > best_w:
        return True
    if new_w == best_w and new_r < best_r:
        return True
    return False


def parse_best_bowling(value: str) -> tuple[int, int]:
    try:
        w, r = value.split("/")
        return int(w), int(r)
    except Exception:
        return 0, 9999


def empty_season_stats(year: int) -> dict:
    return {
        "year": year,
        "team_id": config.OUR_TEAM_ID,
        "matches_played": 0,
        "batting": {
            "innings": 0,
            "not_outs": 0,
            "runs": 0,
            "fours": 0,
            "sixes": 0,
            "highest_score": 0,
            "average": 0.0,
            "fifties": 0,
            "hundreds": 0,
            "ducks": 0,
        },
        "bowling": {
            "_balls": 0,
            "overs": 0.0,
            "maidens": 0,
            "runs_conceded": 0,
            "wickets": 0,
            "average": 0.0,
            "economy": 0.0,
            "best_bowling": "0/0",
            "five_wickets": 0,
        },
        "fielding": {
            "catches": 0,
            "run_outs": 0,
            "stumpings": 0,
        },
    }


# ---------------------------------------------------------------------------
# Identify Poplars players from a match's innings
# ---------------------------------------------------------------------------

def get_our_player_ids_from_innings(innings_list: list) -> set:
    """
    Returns the set of player_ids who are Poplars CC players, identified
    from the innings where our team batted and where our players bowled.
    """
    our_team_id = str(config.OUR_TEAM_ID)
    our_players: set = set()

    for innings in innings_list:
        team_id = str(innings.get("team_id", ""))
        if team_id == our_team_id:
            for batting_perf in innings.get("batting_performances", []):
                pid = str(batting_perf.get("player_id", ""))
                if pid:
                    our_players.add(pid)
        else:
            for bowling_perf in innings.get("bowling_performances", []):
                pid = str(bowling_perf.get("player_id", ""))
                if pid:
                    our_players.add(pid)

    return our_players


# ---------------------------------------------------------------------------
# Season stats - incremental update for one match
# ---------------------------------------------------------------------------

def update_season_stats(db, innings_list: list, season_year: int) -> set:
    """
    Incrementally update season_stats/{year} for each Poplars CC player
    who appeared in this match.

    Reads the existing season_stats document, adds this match's contribution,
    recalculates derived fields, and writes back.

    Returns the set of player_ids that were updated.
    """
    our_team_id = str(config.OUR_TEAM_ID)
    our_players = get_our_player_ids_from_innings(innings_list)
    updated_pids: set = set()

    our_batting_innings = [i for i in innings_list if str(i.get("team_id", "")) == our_team_id]
    our_bowling_innings = [i for i in innings_list if str(i.get("team_id", "")) != our_team_id]

    contributions: dict[str, dict] = {
        pid: {
            "batted": False,
            "runs": 0,
            "innings": 0,
            "not_out": False,
            "highest": 0,
            "fours": 0,
            "sixes": 0,
            "fifty": False,
            "hundred": False,
            "duck": False,
            "bowled": False,
            "bowl_balls": 0,
            "maidens": 0,
            "runs_conceded": 0,
            "wickets": 0,
            "five_wkt": False,
            "best_w": 0,
            "best_r": 9999,
            "catches": 0,
            "stumpings": 0,
            "run_outs": 0,
        }
        for pid in our_players
    }

    for innings in our_batting_innings:
        for batting_perf in innings.get("batting_performances", []):
            pid = str(batting_perf.get("player_id", ""))
            dismissal = batting_perf.get("dismissal_type", "").lower()
            if pid not in contributions or dismissal == "did not bat":
                continue

            runs = safe_int(batting_perf.get("runs", 0))
            contribution = contributions[pid]
            contribution["batted"] = True
            contribution["innings"] += 1
            contribution["runs"] += runs
            contribution["fours"] += safe_int(batting_perf.get("fours", 0))
            contribution["sixes"] += safe_int(batting_perf.get("sixes", 0))
            if dismissal == "not out":
                contribution["not_out"] = True
            elif runs == 0:
                contribution["duck"] = True
            if runs > contribution["highest"]:
                contribution["highest"] = runs
            if runs >= 100:
                contribution["hundred"] = True
            elif runs >= 50:
                contribution["fifty"] = True

    for innings in our_bowling_innings:
        for bowling_perf in innings.get("bowling_performances", []):
            pid = str(bowling_perf.get("player_id", ""))
            if pid not in contributions:
                continue

            overs = safe_float(bowling_perf.get("overs", 0))
            runs_conceded = safe_int(bowling_perf.get("runs_conceded", 0))
            wickets = safe_int(bowling_perf.get("wickets", 0))
            contribution = contributions[pid]
            contribution["bowled"] = True
            contribution["bowl_balls"] += overs_to_balls(overs)
            contribution["maidens"] += safe_int(bowling_perf.get("maidens", 0))
            contribution["runs_conceded"] += runs_conceded
            contribution["wickets"] += wickets
            if wickets >= 5:
                contribution["five_wkt"] = True
            if is_better_bowling(wickets, runs_conceded, contribution["best_w"], contribution["best_r"]):
                contribution["best_w"] = wickets
                contribution["best_r"] = runs_conceded

    for innings in our_bowling_innings:
        for batting_perf in innings.get("batting_performances", []):
            fielder_id = str(batting_perf.get("fielder_id", ""))
            dismissal = batting_perf.get("dismissal_type", "").lower()
            if fielder_id not in contributions:
                continue
            if dismissal == "caught":
                contributions[fielder_id]["catches"] += 1
            elif dismissal == "stumped":
                contributions[fielder_id]["stumpings"] += 1
            elif "run out" in dismissal:
                contributions[fielder_id]["run_outs"] += 1

    for pid, contribution in contributions.items():
        if not contribution["batted"] and not contribution["bowled"] and not any([
            contribution["catches"], contribution["stumpings"], contribution["run_outs"]
        ]):
            continue

        ref = (
            db.collection("players")
            .document(pid)
            .collection("season_stats")
            .document(str(season_year))
        )

        existing_doc = ref.get()
        stats = existing_doc.to_dict() if existing_doc.exists else empty_season_stats(season_year)

        if "_balls" not in stats.get("bowling", {}):
            stats.setdefault("bowling", {})["_balls"] = overs_to_balls(
                safe_float(stats["bowling"].get("overs", 0))
            )

        if contribution["batted"]:
            stats["matches_played"] = stats.get("matches_played", 0) + 1
            batting = stats["batting"]
            batting["innings"] += contribution["innings"]
            batting["runs"] += contribution["runs"]
            batting["fours"] += contribution["fours"]
            batting["sixes"] += contribution["sixes"]
            if contribution["not_out"]:
                batting["not_outs"] += 1
            if contribution["duck"]:
                batting["ducks"] += 1
            if contribution["highest"] > batting["highest_score"]:
                batting["highest_score"] = contribution["highest"]
            if contribution["hundred"]:
                batting["hundreds"] += 1
            elif contribution["fifty"]:
                batting["fifties"] += 1

        if contribution["bowled"]:
            if not contribution["batted"]:
                stats["matches_played"] = stats.get("matches_played", 0) + 1
            bowling = stats["bowling"]
            bowling["_balls"] += contribution["bowl_balls"]
            bowling["maidens"] += contribution["maidens"]
            bowling["runs_conceded"] += contribution["runs_conceded"]
            bowling["wickets"] += contribution["wickets"]
            if contribution["five_wkt"]:
                bowling["five_wickets"] += 1
            best_w, best_r = parse_best_bowling(bowling.get("best_bowling", "0/0"))
            if is_better_bowling(contribution["best_w"], contribution["best_r"], best_w, best_r):
                bowling["best_bowling"] = f"{contribution['best_w']}/{contribution['best_r']}"

        fielding = stats["fielding"]
        fielding["catches"] += contribution["catches"]
        fielding["stumpings"] += contribution["stumpings"]
        fielding["run_outs"] += contribution["run_outs"]

        batting = stats["batting"]
        dismissals = batting["innings"] - batting["not_outs"]
        batting["average"] = safe_divide(batting["runs"], dismissals)

        bowling = stats["bowling"]
        bowling["overs"] = balls_to_overs(bowling.pop("_balls", 0))
        bowling["average"] = safe_divide(bowling["runs_conceded"], bowling["wickets"])
        bowling["economy"] = safe_divide(bowling["runs_conceded"], bowling["overs"])

        stats["last_updated"] = firestore.SERVER_TIMESTAMP
        ref.set(stats, merge=False)
        updated_pids.add(pid)
        log.info("Updated season_stats/%s for player %s", season_year, pid)

    return updated_pids


# ---------------------------------------------------------------------------
# Career stats - full recalculation from all season_stats
# ---------------------------------------------------------------------------

def update_career_stats(db, player_ids: set):
    """
    Full recalculation of career_stats/summary for each player.
    Reads all season_stats documents and sums them.
    Called after season stats have been updated.
    """
    for pid in player_ids:
        season_docs = list(
            db.collection("players")
            .document(pid)
            .collection("season_stats")
            .stream()
        )

        if not season_docs:
            continue

        career = {
            "total_matches": 0,
            "seasons_played": [],
            "batting": {
                "innings": 0,
                "not_outs": 0,
                "runs": 0,
                "fours": 0,
                "sixes": 0,
                "highest_score": 0,
                "average": 0.0,
                "fifties": 0,
                "hundreds": 0,
                "ducks": 0,
            },
            "bowling": {
                "_balls": 0,
                "overs": 0.0,
                "maidens": 0,
                "runs_conceded": 0,
                "wickets": 0,
                "average": 0.0,
                "economy": 0.0,
                "best_bowling": "0/0",
                "five_wickets": 0,
            },
            "fielding": {
                "catches": 0,
                "run_outs": 0,
                "stumpings": 0,
            },
        }

        for doc in season_docs:
            season = doc.to_dict()
            year = season.get("year")
            if year:
                career["seasons_played"].append(year)
            career["total_matches"] += season.get("matches_played", 0)

            batting = season.get("batting", {})
            career_batting = career["batting"]
            career_batting["innings"] += batting.get("innings", 0)
            career_batting["not_outs"] += batting.get("not_outs", 0)
            career_batting["runs"] += batting.get("runs", 0)
            career_batting["fours"] += batting.get("fours", 0)
            career_batting["sixes"] += batting.get("sixes", 0)
            career_batting["fifties"] += batting.get("fifties", 0)
            career_batting["hundreds"] += batting.get("hundreds", 0)
            career_batting["ducks"] += batting.get("ducks", 0)
            if batting.get("highest_score", 0) > career_batting["highest_score"]:
                career_batting["highest_score"] = batting["highest_score"]

            bowling = season.get("bowling", {})
            career_bowling = career["bowling"]
            career_bowling["_balls"] += overs_to_balls(safe_float(bowling.get("overs", 0)))
            career_bowling["maidens"] += bowling.get("maidens", 0)
            career_bowling["runs_conceded"] += bowling.get("runs_conceded", 0)
            career_bowling["wickets"] += bowling.get("wickets", 0)
            career_bowling["five_wickets"] += bowling.get("five_wickets", 0)
            season_w, season_r = parse_best_bowling(bowling.get("best_bowling", "0/0"))
            career_w, career_r = parse_best_bowling(career_bowling.get("best_bowling", "0/0"))
            if is_better_bowling(season_w, season_r, career_w, career_r):
                career_bowling["best_bowling"] = bowling.get("best_bowling", "0/0")

            fielding = season.get("fielding", {})
            career_fielding = career["fielding"]
            career_fielding["catches"] += fielding.get("catches", 0)
            career_fielding["run_outs"] += fielding.get("run_outs", 0)
            career_fielding["stumpings"] += fielding.get("stumpings", 0)

        career["seasons_played"].sort()
        career_batting = career["batting"]
        dismissals = career_batting["innings"] - career_batting["not_outs"]
        career_batting["average"] = safe_divide(career_batting["runs"], dismissals)

        career_bowling = career["bowling"]
        career_bowling["overs"] = balls_to_overs(career_bowling.pop("_balls", 0))
        career_bowling["average"] = safe_divide(career_bowling["runs_conceded"], career_bowling["wickets"])
        career_bowling["economy"] = safe_divide(career_bowling["runs_conceded"], career_bowling["overs"])

        career["last_updated"] = firestore.SERVER_TIMESTAMP

        (
            db.collection("players")
            .document(pid)
            .collection("career_stats")
            .document("summary")
            .set(career, merge=False)
        )
        log.info("Updated career_stats/summary for player %s", pid)
