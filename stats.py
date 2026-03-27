"""
stats.py
Aggregates batting, bowling and fielding statistics from in-memory innings
data and writes the results to Firestore season_stats and career_stats.

Design decision: season stats are computed from all innings held in memory
for the current year rather than incremental per-match updates.  This makes
the backfill idempotent --- running it twice produces the same result.
Career stats are a full recalculation from all season_stats documents.
"""

import logging
from google.cloud import firestore

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bowling overs arithmetic helpers
# Overs in cricket use a non-decimal notation: 8.4 means 8 overs + 4 balls,
# NOT 8.4 decimal overs.  All arithmetic is done in whole balls.
# ---------------------------------------------------------------------------

def overs_to_balls(overs: float) -> int:
    whole   = int(overs)
    partial = round((overs - whole) * 10)  # e.g. 8.4 → 4 balls
    return whole * 6 + partial


def balls_to_overs(balls: int) -> float:
    return float(f"{balls // 6}.{balls % 6}")


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    return round(numerator / denominator, 2) if denominator else default


def is_better_bowling(new_w: int, new_r: int, best_w: int, best_r: int) -> bool:
    """Return True if (new_w / new_r) is a better bowling performance."""
    if new_w > best_w:
        return True
    if new_w == best_w and new_r < best_r:
        return True
    return False


def parse_best_bowling(value: str) -> tuple[int, int]:
    """Parse '5/23' → (5, 23).  Returns (0, 9999) if missing."""
    try:
        w, r = value.split("/")
        return int(w), int(r)
    except Exception:
        return 0, 9999


# ---------------------------------------------------------------------------
# Empty stats template
# ---------------------------------------------------------------------------

def empty_player_stats() -> dict:
    return {
        "matches_played": 0,
        "team_id": "",
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
            "_balls":        0,       # internal: removed before writing
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
# Single-innings accumulators
# ---------------------------------------------------------------------------

def accumulate_batting(stats: dict, b: dict):
    """Add one batting performance entry to a player's in-memory stats."""
    dismissal = b.get("dismissal_type", "")
    if dismissal == "did not bat":
        return

    runs = b.get("runs", 0)
    stats["batting"]["innings"] += 1
    stats["batting"]["runs"]    += runs

    if dismissal == "not out":
        stats["batting"]["not_outs"] += 1
    elif runs == 0:
        stats["batting"]["ducks"] += 1

    if runs > stats["batting"]["highest_score"]:
        stats["batting"]["highest_score"] = runs
    if runs >= 100:
        stats["batting"]["hundreds"] += 1
    elif runs >= 50:
        stats["batting"]["fifties"] += 1


def accumulate_bowling(stats: dict, bw: dict):
    """Add one bowling performance entry to a player's in-memory stats."""
    overs         = bw.get("overs", 0.0)
    wickets       = bw.get("wickets", 0)
    runs_conceded = bw.get("runs_conceded", 0)

    stats["bowling"]["_balls"]        += overs_to_balls(overs)
    stats["bowling"]["maidens"]       += bw.get("maidens", 0)
    stats["bowling"]["runs_conceded"] += runs_conceded
    stats["bowling"]["wickets"]       += wickets

    if wickets >= 5:
        stats["bowling"]["five_wickets"] += 1

    best_w, best_r = parse_best_bowling(stats["bowling"]["best_bowling"])
    if is_better_bowling(wickets, runs_conceded, best_w, best_r):
        stats["bowling"]["best_bowling"] = f"{wickets}/{runs_conceded}"


def accumulate_fielding(stats: dict, b: dict, player_id: str):
    """
    Credit a fielding action to a player based on the batting dismissal record.
    fielder_id in the batting performance object is the player who completed
    the dismissal (catcher, stumper, or run-out fielder).
    """
    if str(b.get("fielder_id", "")) != player_id:
        return
    dismissal = b.get("dismissal_type", "")
    if dismissal == "caught":
        stats["fielding"]["catches"]   += 1
    elif dismissal == "stumped":
        stats["fielding"]["stumpings"] += 1
    elif dismissal == "run out":
        stats["fielding"]["run_outs"]  += 1


# ---------------------------------------------------------------------------
# Derived field computation
# ---------------------------------------------------------------------------

def compute_derived_fields(stats: dict):
    """Recalculate all average / economy fields from raw totals."""
    b  = stats["batting"]
    bw = stats["bowling"]

    dismissals   = b["innings"] - b["not_outs"]
    b["average"] = safe_divide(b["runs"], dismissals)

    # Convert internal _balls counter to overs notation
    bw["overs"]   = balls_to_overs(bw.pop("_balls", 0))
    bw["average"] = safe_divide(bw["runs_conceded"], bw["wickets"])
    bw["economy"] = safe_divide(bw["runs_conceded"], bw["overs"])


# ---------------------------------------------------------------------------
# Main entry points called from main.py
# ---------------------------------------------------------------------------

def build_season_stats(year: int, all_innings: list) -> dict:
    """
    Build a {player_id: stats_dict} map for a given season by iterating
    over all innings documents collected in memory.  No Firestore reads.

    Parameters
    ----------
    year        : the season year (stored in the resulting documents)
    all_innings : list of (match_id, innings_dict) tuples for the year
    """
    player_stats: dict[str, dict] = {}

    for match_id, innings in all_innings:
        team_id = innings.get("team_id", "")

        for b in innings.get("batting_performances", []):
            pid = b.get("player_id", "")
            if not pid:
                continue
            if pid not in player_stats:
                player_stats[pid] = empty_player_stats()
            player_stats[pid]["team_id"] = team_id
            accumulate_batting(player_stats[pid], b)

        for bw in innings.get("bowling_performances", []):
            pid = bw.get("player_id", "")
            if not pid:
                continue
            if pid not in player_stats:
                player_stats[pid] = empty_player_stats()
            accumulate_bowling(player_stats[pid], bw)

        # Fielding: credit the player named in each batting dismissal
        for b in innings.get("batting_performances", []):
            fielder_id = b.get("fielder_id", "")
            if not fielder_id:
                continue
            if fielder_id not in player_stats:
                player_stats[fielder_id] = empty_player_stats()
            accumulate_fielding(player_stats[fielder_id], b, fielder_id)

    # Count matches appeared in (a player appeared if they have any innings)
    # We derive this from innings --- one match = at most 1 batting + 1 bowling
    # appearance, so we track unique match_ids per player separately.
    player_matches: dict[str, set] = {}
    for match_id, innings in all_innings:
        participants = set()
        for b  in innings.get("batting_performances", []):
            if b.get("player_id"):
                participants.add(b["player_id"])
        for bw in innings.get("bowling_performances", []):
            if bw.get("player_id"):
                participants.add(bw["player_id"])
        for b in innings.get("batting_performances", []):
            if b.get("fielder_id"):
                participants.add(b["fielder_id"])
        for pid in participants:
            player_matches.setdefault(pid, set()).add(match_id)

    for pid, stats in player_stats.items():
        stats["matches_played"] = len(player_matches.get(pid, set()))
        stats["year"]           = year
        compute_derived_fields(stats)

    return player_stats


def write_season_stats(db, player_stats: dict[str, dict]):
    """Write each player's season stats dict to Firestore."""
    for player_id, stats in player_stats.items():
        year = stats.get("year")
        if not year:
            continue
        stats["last_updated"] = firestore.SERVER_TIMESTAMP
        ref = (
            db.collection("players")
              .document(player_id)
              .collection("season_stats")
              .document(str(year))
        )
        ref.set(stats, merge=False)
    log.info(f"  Season stats written for {len(player_stats)} players")


def write_career_stats(db, player_ids: set):
    """
    Full recalculation of career_stats/summary for each player.
    Reads all season_stats documents for the player and sums them.
    """
    for player_id in player_ids:
        season_docs = (
            db.collection("players")
              .document(player_id)
              .collection("season_stats")
              .stream()
        )
        seasons = [s.to_dict() for s in season_docs]
        if not seasons:
            continue

        career = empty_player_stats()
        career["seasons_played"] = []
        career["total_matches"]  = 0

        for s in seasons:
            career["total_matches"] += s.get("matches_played", 0)
            year = s.get("year")
            if year:
                career["seasons_played"].append(year)

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

            # Bowling --- accumulate in balls
            bw  = s.get("bowling", {})
            cbw = career["bowling"]
            cbw["_balls"]        += overs_to_balls(bw.get("overs", 0))
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

        career["seasons_played"].sort()
        compute_derived_fields(career)
        career["last_updated"] = firestore.SERVER_TIMESTAMP

        ref = (
            db.collection("players")
              .document(player_id)
              .collection("career_stats")
              .document("summary")
        )
        ref.set(career, merge=False)

    log.info(f"  Career stats written for {len(player_ids)} players")
