"""
transform.py
Converts raw Play-Cricket API responses into the Firestore document shapes
defined in the design document.  No Firestore calls happen here --- this
module is pure data transformation so it is easy to unit-test.
"""

from datetime import datetime
from google.cloud import firestore


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def parse_date(date_str: str):
    """Try several Play-Cricket date formats and return a datetime or None."""
    if not date_str:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d %b %Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


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


def compute_strike_rate(runs: int, balls: int) -> float:
    return round((runs / balls) * 100, 2) if balls else 0.0


def compute_economy(runs: int, overs: float) -> float:
    return round(runs / overs, 2) if overs else 0.0


# ---------------------------------------------------------------------------
# Dismissal helpers
# ---------------------------------------------------------------------------

def get_dismissal_type(how_out: str) -> str:
    """
    Derive a clean dismissal_type string from Play-Cricket's how_out text.
    Examples of how_out values:  'c Smith b Jones', 'b Jones', 'lbw b Jones',
    'run out (Brown)', 'st †Smith b Jones', 'not out', 'did not bat'
    """
    if not how_out:
        return "did not bat"
    h = how_out.lower().strip()
    if h in ("not out", ""):
        return "not out"
    if h == "did not bat":
        return "did not bat"
    if h.startswith("c ") or h.startswith("c&b") or h == "caught":
        return "caught"
    if h.startswith("b ") or h == "bowled":
        return "bowled"
    if h.startswith("lbw"):
        return "lbw"
    if "run out" in h:
        return "run out"
    if h.startswith("st ") or h == "stumped":
        return "stumped"
    if "retired" in h:
        return "retired"
    return "out"


# ---------------------------------------------------------------------------
# Match-level result helpers
# ---------------------------------------------------------------------------

def get_result_from_perspective(detail: dict, our_team_id: str) -> tuple[str, str]:
    """
    Returns (result, result_description) from our club's perspective.
    Play-Cricket result codes: W = win, L = loss, D = draw,
                               T = tied, A = abandoned, C = conceded
    result_applied_to indicates which team the result code belongs to.
    """
    result_code       = str(detail.get("result", "")).upper()
    result_applied_to = str(detail.get("result_applied_to", ""))
    win_by_runs       = safe_int(detail.get("win_by_runs", 0))
    win_by_wickets    = safe_int(detail.get("win_by_wickets", 0))

    if result_code == "A":
        return "no result", "Match abandoned"
    if result_code == "C":
        return "no result", "Match conceded"
    if result_code == "D":
        return "draw", "Match drawn"
    if result_code == "T":
        return "tied", "Match tied"

    if result_code == "W":
        won = result_applied_to == our_team_id
        if won:
            if win_by_runs > 0:
                return "won", f"Won by {win_by_runs} runs"
            if win_by_wickets > 0:
                return "won", f"Won by {win_by_wickets} wickets"
            return "won", "Won"
        else:
            if win_by_runs > 0:
                return "lost", f"Lost by {win_by_runs} runs"
            if win_by_wickets > 0:
                return "lost", f"Lost by {win_by_wickets} wickets"
            return "lost", "Lost"

    return "", ""


# ---------------------------------------------------------------------------
# Document transformers
# ---------------------------------------------------------------------------

def transform_match(detail: dict, our_team_id: str, season_year: int) -> dict:
    """
    Transform a Play-Cricket match_detail response into the Firestore
    matches/{match_id} document shape.
    """
    competition_type = detail.get("competition_type", "")
    match_type       = "league" if competition_type == "L" else "friendly"
    home_team_id     = str(detail.get("home_team_id", ""))
    result, result_description = get_result_from_perspective(detail, our_team_id)

    return {
        "match_id":           str(detail.get("id", "")),
        "site_id":            str(detail.get("site_id", "")),
        "team_id":            our_team_id,
        "season_year":        season_year,
        "match_type":         match_type,
        "competition_name":   detail.get("competition_name", ""),
        "competition_id":     str(detail.get("competition_id", "")),
        "division":           detail.get("division", ""),
        "date":               parse_date(detail.get("match_date", "")),
        "venue":              detail.get("ground_name", ""),
        "is_home_game":       home_team_id == our_team_id,
        "home_team": {
            "id":   home_team_id,
            "name": detail.get("home_team_name", ""),
        },
        "away_team": {
            "id":   str(detail.get("away_team_id", "")),
            "name": detail.get("away_team_name", ""),
        },
        "toss_won_by":        detail.get("toss_won_by_name", ""),
        "toss_decision":      detail.get("toss_decision", ""),
        "result":             result,
        "result_description": result_description,
        "last_updated":       firestore.SERVER_TIMESTAMP,
    }


def transform_innings(innings_data: dict) -> dict:
    """
    Transform one innings block from the Play-Cricket match_detail response
    into the Firestore matches/{id}/innings/{1 or 2} document shape.
    Batting and bowling performances are embedded arrays as per the design.
    """
    batting_perfs = []
    for b in innings_data.get("batsmen", []):
        runs     = safe_int(b.get("runs", 0))
        balls    = safe_int(b.get("balls", 0))
        how_out  = b.get("how_out", "")
        batting_perfs.append({
            "player_id":       str(b.get("batsman_id", "")),
            "player_name":     b.get("batsman_name", ""),
            "batting_position": safe_int(b.get("position", 0)),
            "runs":            runs,
            "balls_faced":     balls,
            "fours":           safe_int(b.get("fours", 0)),
            "sixes":           safe_int(b.get("sixes", 0)),
            "strike_rate":     compute_strike_rate(runs, balls),
            "how_out":         how_out,
            "dismissal_type":  get_dismissal_type(how_out),
            # fielder_id is used by stats.py to credit catches/stumpings/run-outs
            "fielder_id":      str(b.get("fielder_id", "")),
        })

    bowling_perfs = []
    for bw in innings_data.get("bowlers", []):
        runs_conceded = safe_int(bw.get("runs_conceded", 0))
        overs         = safe_float(bw.get("overs", 0))
        bowling_perfs.append({
            "player_id":     str(bw.get("bowler_id", "")),
            "player_name":   bw.get("bowler_name", ""),
            "overs":         overs,
            "maidens":       safe_int(bw.get("maidens", 0)),
            "runs_conceded": runs_conceded,
            "wickets":       safe_int(bw.get("wickets", 0)),
            "wides":         safe_int(bw.get("wides", 0)),
            "no_balls":      safe_int(bw.get("no_balls", 0)),
            "economy":       compute_economy(runs_conceded, overs),
        })

    fow = []
    for f in innings_data.get("fow", []):
        fow.append({
            "wicket_number": safe_int(f.get("wicket_number", 0)),
            "runs":          safe_int(f.get("runs", 0)),
            "player_name":   f.get("batsman_out_name", ""),
        })

    wides   = safe_int(innings_data.get("extra_wides", 0))
    no_balls = safe_int(innings_data.get("extra_no_balls", 0))
    byes    = safe_int(innings_data.get("extra_byes", 0))
    leg_byes = safe_int(innings_data.get("extra_leg_byes", 0))

    return {
        "innings_number":      safe_int(innings_data.get("innings_number", 0)),
        "team_id":             str(innings_data.get("team_batting_id", "")),
        "team_name":           innings_data.get("team_batting_name", ""),
        "total_runs":          safe_int(innings_data.get("runs", 0)),
        "wickets":             safe_int(innings_data.get("wickets", 0)),
        "overs":               safe_float(innings_data.get("overs", 0)),
        "extras": {
            "wides":    wides,
            "no_balls": no_balls,
            "byes":     byes,
            "leg_byes": leg_byes,
            "total":    wides + no_balls + byes + leg_byes,
        },
        "batting_performances":  batting_perfs,
        "bowling_performances":  bowling_perfs,
        "fall_of_wickets":       fow,
    }
