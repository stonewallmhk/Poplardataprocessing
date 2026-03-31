"""
stats_engine.py
Incremental stats computation engine used by 04_sync.py.

Called after every new match is written to Firestore:
  1. update_season_stats() — adds this match's contribution to each
     player's season_stats/{year} document
  2. update_career_stats() — full recalculation of career_stats/summary
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
    whole   = int(overs)
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
        "year":           year,
        "team_id":        config.OUR_TEAM_ID,
        "matches_played": 0,
        "batting": {
            "innings":       0,
            "not_outs":      0,
            "runs":          0,
            "highest_score": 0,
            "average":       0.0,
            "fifties":       0,
            "hundreds":      0,
            "ducks":         0,
        },
        "bowling": {
            "_balls":        0,
            "overs":         0.0,
            "maidens":       0,
            "runs_conceded": 0,
            "wickets":       0,
            "average":       0.0,
            "economy":       0.0,
            "best_bowling":  "0/0",
            "five_wickets":  0,
        },
        "fielding": {
            "catches":   0,
            "run_outs":  0,
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
    our_team_id  = str(config.OUR_TEAM_ID)
    our_players: set = set()

    for innings in innings_list:
        team_id = str(innings.get("team_id", ""))
        if team_id == our_team_id:
            # Our batting innings — collect batting player IDs
            for b in innings.get("batting_performances", []):
                pid = str(b.get("player_id", ""))
                if pid:
                    our_players.add(pid)
        else:
            # Opponent batting innings — collect our bowling player IDs
            for bw in innings.get("bowling_performances", []):
                pid = str(bw.get("player_id", ""))
                if pid:
                    our_players.add(pid)

    return our_players


# ---------------------------------------------------------------------------
# Season stats — incremental update for one match
# ---------------------------------------------------------------------------

def update_season_stats(db, innings_list: list, season_year: int) -> set:
    """
    Incrementally update season_stats/{year} for each Poplars CC player
    who appeared in this match.

    Reads the existing season_stats document, adds this match's contribution,
    recalculates derived fields, and writes back.

    Returns the set of player_ids that were updated.
    """
    our_team_id  = str(config.OUR_TEAM_ID)
    our_players  = get_our_player_ids_from_innings(innings_list)
    updated_pids: set = set()

    # Group innings by batting team
    our_batting_innings  = [i for i in innings_list if str(i.get("team_id", "")) == our_team_id]
    our_bowling_innings  = [i for i in innings_list if str(i.get("team_id", "")) != our_team_id]

    # Collect per-player contributions from this match
    contributions: dict[str, dict] = {pid: {
        "batted":        False,
        "runs":          0,
        "innings":       0,
        "not_out":       False,
        "highest":       0,
        "fours":         0,
        "sixes":         0,
        "fifty":         False,
        "hundred":       False,
        "duck":          False,
        "bowled":        False,
        "bowl_balls":    0,
        "maidens":       0,
        "runs_conceded": 0,
        "wickets":       0,
        "five_wkt":      False,
        "best_w":        0,
        "best_r":        9999,
        "catches":       0,
        "stumpings":     0,
        "run_outs":      0,
    } for pid in our_players}

    # ---- Batting -----------------------------------------------------------
    for innings in our_batting_innings:
        for b in innings.get("batting_performances", []):
            pid       = str(b.get("player_id", ""))
            dismissal = b.get("dismissal_type", "").lower()
            if pid not in contributions or dismissal == "did not bat":
                continue

            runs = safe_int(b.get("runs", 0))
            c    = contributions[pid]
            c["batted"]  = True
            c["innings"] += 1
            c["runs"]    += runs
            if dismissal == "not out":
                c["not_out"] = True
            elif runs == 0:
                c["duck"] = True
            if runs > c["highest"]:
                c["highest"] = runs
            if runs >= 100:
                c["hundred"] = True
            elif runs >= 50:
                c["fifty"] = True

    # ---- Bowling -----------------------------------------------------------
    for innings in our_bowling_innings:
        for bw in innings.get("bowling_performances", []):
            pid = str(bw.get("player_id", ""))
            if pid not in contributions:
                continue

            overs         = safe_float(bw.get("overs", 0))
            runs_conceded = safe_int(bw.get("runs_conceded", 0))
            wickets       = safe_int(bw.get("wickets", 0))
            c             = contributions[pid]
            c["bowled"]        = True
            c["bowl_balls"]   += overs_to_balls(overs)
            c["maidens"]      += safe_int(bw.get("maidens", 0))
            c["runs_conceded"]+= runs_conceded
            c["wickets"]      += wickets
            if wickets >= 5:
                c["five_wkt"] = True
            if is_better_bowling(wickets, runs_conceded, c["best_w"], c["best_r"]):
                c["best_w"] = wickets
                c["best_r"] = runs_conceded

    # ---- Fielding ----------------------------------------------------------
    for innings in our_bowling_innings:
        for b in innings.get("batting_performances", []):
            fielder_id = str(b.get("fielder_id", ""))
            dismissal  = b.get("dismissal_type", "").lower()
            if fielder_id not in contributions:
                continue
            if dismissal == "caught":
                contributions[fielder_id]["catches"]   += 1
            elif dismissal == "stumped":
                contributions[fielder_id]["stumpings"] += 1
            elif "run out" in dismissal:
                contributions[fielder_id]["run_outs"]  += 1

    # ---- Write incremental updates to Firestore ----------------------------
    for pid, c in contributions.items():
        # Skip players who neither batted, bowled, nor fielded
        if not c["batted"] and not c["bowled"] and not any([
            c["catches"], c["stumpings"], c["run_outs"]
        ]):
            continue

        ref = (
            db.collection("players")
              .document(pid)
              .collection("season_stats")
              .document(str(season_year))
        )

        # Read existing or start fresh
        existing_doc = ref.get()
        stats = existing_doc.to_dict() if existing_doc.exists else empty_season_stats(season_year)

        # Ensure _balls field exists for bowling accumulation
        if "_balls" not in stats.get("bowling", {}):
            stats.setdefault("bowling", {})["_balls"] = overs_to_balls(
                safe_float(stats["bowling"].get("overs", 0))
            )

        # Apply batting contribution
        if c["batted"]:
            stats["matches_played"] = stats.get("matches_played", 0) + 1
            b = stats["batting"]
            b["innings"]    += c["innings"]
            b["runs"]       += c["runs"]
            if c["not_out"]:
                b["not_outs"] += 1
            if c["duck"]:
                b["ducks"]    += 1
            if c["highest"] > b["highest_score"]:
                b["highest_score"] = c["highest"]
            if c["hundred"]:
                b["hundreds"] += 1
            elif c["fifty"]:
                b["fifties"]  += 1

        # Apply bowling contribution
        if c["bowled"]:
            if not c["batted"]:
                stats["matches_played"] = stats.get("matches_played", 0) + 1
            bw = stats["bowling"]
            bw["_balls"]        += c["bowl_balls"]
            bw["maidens"]       += c["maidens"]
            bw["runs_conceded"] += c["runs_conceded"]
            bw["wickets"]       += c["wickets"]
            if c["five_wkt"]:
                bw["five_wickets"] += 1
            best_w, best_r = parse_best_bowling(bw.get("best_bowling", "0/0"))
            if is_better_bowling(c["best_w"], c["best_r"], best_w, best_r):
                bw["best_bowling"] = f"{c['best_w']}/{c['best_r']}"

        # Apply fielding contribution
        f = stats["fielding"]
        f["catches"]   += c["catches"]
        f["stumpings"] += c["stumpings"]
        f["run_outs"]  += c["run_outs"]

        # Recalculate derived fields
        bat        = stats["batting"]
        dismissals = bat["innings"] - bat["not_outs"]
        bat["average"] = safe_divide(bat["runs"], dismissals)

        bwl = stats["bowling"]
        bwl["overs"]   = balls_to_overs(bwl.pop("_balls", 0))
        bwl["average"] = safe_divide(bwl["runs_conceded"], bwl["wickets"])
        bwl["economy"] = safe_divide(bwl["runs_conceded"], bwl["overs"])

        stats["last_updated"] = firestore.SERVER_TIMESTAMP
        ref.set(stats, merge=False)
        updated_pids.add(pid)
        log.info(f"Updated season_stats/{season_year} for player {pid}")

    return updated_pids


# ---------------------------------------------------------------------------
# Career stats — full recalculation from all season_stats
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
            "total_matches":  0,
            "seasons_played": [],
            "batting": {
                "innings": 0, "not_outs": 0, "runs": 0,
                "highest_score": 0, "average": 0.0,
                "fifties": 0, "hundreds": 0, "ducks": 0,
            },
            "bowling": {
                "_balls": 0, "overs": 0.0, "maidens": 0,
                "runs_conceded": 0, "wickets": 0,
                "average": 0.0, "economy": 0.0,
                "best_bowling": "0/0", "five_wickets": 0,
            },
            "fielding": {"catches": 0, "run_outs": 0, "stumpings": 0},
        }

        for doc in season_docs:
            s = doc.to_dict()
            year = s.get("year")
            if year:
                career["seasons_played"].append(year)
            career["total_matches"] += s.get("matches_played", 0)

            # Batting
            b  = s.get("batting", {})
            cb = career["batting"]
            cb["innings"]    += b.get("innings", 0)
            cb["not_outs"]   += b.get("not_outs", 0)
            cb["runs"]       += b.get("runs", 0)
            cb["fifties"]    += b.get("fifties", 0)
            cb["hundreds"]   += b.get("hundreds", 0)
            cb["ducks"]      += b.get("ducks", 0)
            if b.get("highest_score", 0) > cb["highest_score"]:
                cb["highest_score"] = b["highest_score"]

            # Bowling
            bw  = s.get("bowling", {})
            cbw = career["bowling"]
            cbw["_balls"]        += overs_to_balls(safe_float(bw.get("overs", 0)))
            cbw["maidens"]       += bw.get("maidens", 0)
            cbw["runs_conceded"] += bw.get("runs_conceded", 0)
            cbw["wickets"]       += bw.get("wickets", 0)
            cbw["five_wickets"]  += bw.get("five_wickets", 0)
            s_w, s_r = parse_best_bowling(bw.get("best_bowling", "0/0"))
            c_w, c_r = parse_best_bowling(cbw.get("best_bowling", "0/0"))
            if is_better_bowling(s_w, s_r, c_w, c_r):
                cbw["best_bowling"] = bw.get("best_bowling", "0/0")

            # Fielding
            f  = s.get("fielding", {})
            cf = career["fielding"]
            cf["catches"]   += f.get("catches", 0)
            cf["run_outs"]  += f.get("run_outs", 0)
            cf["stumpings"] += f.get("stumpings", 0)

        # Derive final fields
        career["seasons_played"].sort()
        cb  = career["batting"]
        dismissals  = cb["innings"] - cb["not_outs"]
        cb["average"] = safe_divide(cb["runs"], dismissals)

        cbw = career["bowling"]
        cbw["overs"]   = balls_to_overs(cbw.pop("_balls", 0))
        cbw["average"] = safe_divide(cbw["runs_conceded"], cbw["wickets"])
        cbw["economy"] = safe_divide(cbw["runs_conceded"], cbw["overs"])

        career["last_updated"] = firestore.SERVER_TIMESTAMP

        (
            db.collection("players")
              .document(pid)
              .collection("career_stats")
              .document("summary")
              .set(career, merge=False)
        )
        log.info(f"Updated career_stats/summary for player {pid}")
