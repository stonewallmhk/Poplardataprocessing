"""
api_helpers.py
Shared Play-Cricket API fetch functions and Firestore document transformers.
Used by both 03_matches.py (interactive) and 04_sync.py (automated sync).

No direct Firestore writes here — all writes happen in the calling script
so that each script controls its own confirmation and logging.
"""

import logging
import time
import requests
from datetime import datetime
from google.cloud import firestore
import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# General helpers
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


def parse_date(date_str: str):
    """Parse Play-Cricket date format dd/mm/yyyy into a Python datetime."""
    if not date_str:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def expand_how_out(code: str) -> str:
    """
    Convert Play-Cricket how_out values to full dismissal type strings.
    The API returns a mix of short codes and full strings.
    """
    mapping = {
        "b":          "bowled",
        "ct":         "caught",
        "no":         "not out",
        "not out":    "not out",
        "lbw":        "lbw",
        "ro":         "run out",
        "run out":    "run out",
        "st":         "stumped",
        "dnb":        "did not bat",
        "did not bat":"did not bat",
        "ret":        "retired",
        "retired":    "retired",
        "hit":        "hit wicket",
        "ob":         "obstructing the field",
    }
    return mapping.get(str(code).strip().lower(), str(code).lower())


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def fetch_match_summaries(api_key: str, season: str) -> list:
    """
    Call Match Summary API and return list of match dicts.
    GET /api/v2/matches.json?site_id=xxx&season=yyyy
    """
    url    = f"{config.PLAY_CRICKET_BASE_URL}/matches.json"
    params = {
        "api_token": api_key,
        "site_id":   config.PLAY_CRICKET_SITE_ID,
        "season":    season,
    }
    log.info(f"Calling Match Summary API for season {season}")
    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        log.error(f"Match Summary API returned HTTP {response.status_code}: {response.text[:300]}")
        return []

    matches = response.json().get("matches", [])
    log.info(f"Match Summary API returned {len(matches)} matches")
    return matches


def fetch_match_detail(api_key: str, match_id: str) -> dict | None:
    """
    Call Match Detail API for a single match.
    GET /api/v2/match_detail.json?match_id=xxx
    Returns the first element of match_details array, or None on failure.
    """
    url    = f"{config.PLAY_CRICKET_BASE_URL}/match_detail.json"
    params = {"api_token": api_key, "match_id": match_id}

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        log.warning(f"Match Detail API returned HTTP {response.status_code} for match {match_id}")
        return None

    details = response.json().get("match_details", [])
    return details[0] if details else None


def is_completed(detail: dict) -> bool:
    """Return True if the match has a result (not a future fixture)."""
    return bool(detail.get("result", "").strip())


# ---------------------------------------------------------------------------
# Document transformers
# ---------------------------------------------------------------------------

def transform_match(detail: dict, season_year: int) -> dict:
    """
    Transform a Match Detail API response into the Firestore
    matches/{match_id} document shape.
    match_result_types and players arrays are intentionally excluded.
    """
    comp_type = str(detail.get("competition_type", "")).lower()
    if comp_type == "league":
        match_type = "league"
    elif comp_type == "friendly":
        match_type = "friendly"
    else:
        match_type = comp_type

    return {
        "match_id":            str(detail.get("id", "")),
        "site_id":             str(config.PLAY_CRICKET_SITE_ID),
        "season_year":         season_year,
        "status":              detail.get("status", ""),
        "match_type":          match_type,
        "competition_name":    detail.get("competition_name", ""),
        "competition_id":      str(detail.get("competition_id", "")),
        "competition_type":    detail.get("competition_type", ""),
        "league_name":         detail.get("league_name", ""),
        "league_id":           str(detail.get("league_id", "")),
        "division":            detail.get("competition_name", ""),
        "date":                parse_date(detail.get("match_date", "")),
        "match_time":          detail.get("match_time", ""),
        "venue":               detail.get("ground_name", ""),
        "ground_id":           str(detail.get("ground_id", "")),
        "no_of_overs":         detail.get("no_of_overs", ""),
        "home_team": {
            "id":        str(detail.get("home_team_id", "")),
            "name":      detail.get("home_team_name", ""),
            "club_name": detail.get("home_club_name", ""),
            "club_id":   str(detail.get("home_club_id", "")),
        },
        "away_team": {
            "id":        str(detail.get("away_team_id", "")),
            "name":      detail.get("away_team_name", ""),
            "club_name": detail.get("away_club_name", ""),
            "club_id":   str(detail.get("away_club_id", "")),
        },
        "match_officials": {
            "umpire_home":   detail.get("umpire_1_name", ""),
            "umpire_home_id":  str(detail.get("umpire_1_id", "")),
            "umpire_away":   detail.get("umpire_2_name", ""),
            "umpire_away_id":  str(detail.get("umpire_2_id", "")),
            "referee":       detail.get("referee_name", ""),
            "referee_id":    str(detail.get("referee_id", "")),
            "scorer_home":   detail.get("scorer_1_name", ""),
            "scorer_home_id":  str(detail.get("scorer_1_id", "")),
            "scorer_away":   detail.get("scorer_2_name", ""),
            "scorer_away_id":  str(detail.get("scorer_2_id", "")),
        },
        "toss_won_by_team_id": str(detail.get("toss_won_by_team_id", "")),
        "toss":                detail.get("toss", ""),
        "batted_first":        str(detail.get("batted_first", "")),
        "result":              detail.get("result", ""),
        "result_description":  detail.get("result_description", ""),
        "result_applied_to":   str(detail.get("result_applied_to", "")),
        "match_notes":         detail.get("match_notes", ""),
        "last_updated":        firestore.SERVER_TIMESTAMP,
    }


def transform_innings(innings_data: dict) -> dict:
    """
    Transform one innings block from the Match Detail API response into the
    Firestore matches/{id}/innings/{n} document shape.
    """
    batting_perfs = []
    for b in innings_data.get("bat", []):
        runs  = safe_int(b.get("runs", 0))
        balls = safe_int(b.get("balls", 0))
        batting_perfs.append({
            "player_id":        str(b.get("batsman_id", "")),
            "player_name":      b.get("batsman_name", ""),
            "batting_position": safe_int(b.get("position", 0)),
            "runs":             runs,
            "balls_faced":      balls,
            "fours":            safe_int(b.get("fours", 0)),
            "sixes":            safe_int(b.get("sixes", 0)),
            "strike_rate":      round((runs / balls) * 100, 2) if balls else 0.0,
            "how_out":          b.get("how_out", ""),
            "dismissal_type":   expand_how_out(b.get("how_out", "")),
            "fielder_name":     b.get("fielder_name", ""),
            "fielder_id":       str(b.get("fielder_id", "")),
            "bowler_name":      b.get("bowler_name", ""),
            "bowler_id":        str(b.get("bowler_id", "")),
        })

    bowling_perfs = []
    for bw in innings_data.get("bowl", []):
        runs_conceded = safe_int(bw.get("runs", 0))   # API field is 'runs' not 'runs_conceded'
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
            "economy":       round(runs_conceded / overs, 2) if overs else 0.0,
        })

    fow = []
    for f in innings_data.get("fow", []):
        fow.append({
            "wicket_number":    safe_int(f.get("wickets", 0)),
            "runs":             safe_int(f.get("runs", 0)),
            "batsman_out_name": f.get("batsman_out_name", ""),
            "batsman_out_id":   str(f.get("batsman_out_id", "")),
            "batsman_in_name":  f.get("batsman_in_name", ""),
            "batsman_in_id":    str(f.get("batsman_in_id", "")),
        })

    wides    = safe_int(innings_data.get("extra_wides", 0))
    no_balls = safe_int(innings_data.get("extra_no_balls", 0))
    byes     = safe_int(innings_data.get("extra_byes", 0))
    leg_byes = safe_int(innings_data.get("extra_leg_byes", 0))

    return {
        "innings_number":      safe_int(innings_data.get("innings_number", 0)),
        "team_id":             str(innings_data.get("team_batting_id", "")),
        "team_name":           innings_data.get("team_batting_name", ""),
        "total_runs":          safe_int(innings_data.get("runs", 0)),
        "wickets":             safe_int(innings_data.get("wickets", 0)),
        "overs":               safe_float(innings_data.get("overs", 0)),
        "declared":            innings_data.get("declared", False),
        "extras": {
            "wides":    wides,
            "no_balls": no_balls,
            "byes":     byes,
            "leg_byes": leg_byes,
            "total":    safe_int(innings_data.get("total_extras", 0)) or (wides + no_balls + byes + leg_byes),
        },
        "batting_performances":  batting_perfs,
        "bowling_performances":  bowling_perfs,
        "fall_of_wickets":       fow,
    }


# ---------------------------------------------------------------------------
# Firestore write — match + innings
# ---------------------------------------------------------------------------

def write_match_to_firestore(db, detail: dict, season_year: int) -> tuple[str, list]:
    """
    Write the match document and both innings subcollection documents.
    Returns (match_id, list_of_innings_dicts) for stats computation.
    Uses array index (1, 2) as innings document ID — not innings_number
    from the API which can be 1 for both innings in a one-innings match.
    """
    match_id  = str(detail.get("id", ""))
    match_doc = transform_match(detail, season_year)

    db.collection("matches").document(match_id).set(match_doc)

    innings_list = []
    for idx, raw_innings in enumerate(detail.get("innings", []), 1):
        innings_doc = transform_innings(raw_innings)
        innings_num = str(idx)   # array position not innings_number field
        (
            db.collection("matches")
              .document(match_id)
              .collection("innings")
              .document(innings_num)
              .set(innings_doc)
        )
        innings_list.append(innings_doc)

    log.info(f"Written match {match_id} with {len(innings_list)} innings")
    return match_id, innings_list


def get_existing_match_ids(db, season_year: int) -> set:
    """
    Return the set of match_ids already stored in Firestore for a given season.
    Used by the sync to compute the diff against the API response.
    """
    docs = (
        db.collection("matches")
          .where("season_year", "==", season_year)
          .stream()
    )
    return {doc.id for doc in docs}
