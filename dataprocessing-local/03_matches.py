"""
03_matches.py
Fetches match summaries and full match details from the Play-Cricket API
and writes them to Firestore matches/ and matches/{id}/innings/ collections.

Run with:
    python 03_matches.py

What it does:
  1. Asks which season year to process
  2. Calls Match Summary API to get all match IDs for that season
  3. For each match ID, calls Match Detail API for the full scorecard
  4. Prints each match for inspection before writing
  5. Writes match document + innings subcollection documents to Firestore

API notes:
  - Match Summary: GET /api/v2/matches.json?site_id=xxx&season=yyyy
  - Match Detail:  GET /api/v2/match_detail.json?match_id=xxx
  - Batting array field is 'bat' (not 'batsmen')
  - Bowling array field is 'bowl' (not 'bowlers')
  - how_out uses short codes: 'b'=bowled, 'ct'=caught, 'no'=not out, etc.
  - result_description comes ready-made from the API
  - match_details response is an array — take index [0]
"""

import time
import requests
from google.cloud import firestore
import config

# ---------------------------------------------------------------------------
# Step 0 — Validate config
# ---------------------------------------------------------------------------
config.validate()

# Delay between Match Detail API calls to respect rate limits
API_DELAY_SECONDS = 0.5


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


def parse_date(date_str: str):
    """Parse Play-Cricket date format dd/mm/yyyy into a Python datetime."""
    from datetime import datetime
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
    Convert Play-Cricket short how_out codes to full dismissal type strings.
    Codes seen in API: 'b'=bowled, 'ct'=caught, 'no'=not out,
    'lbw'=lbw, 'ro'=run out, 'st'=stumped, 'dnb'=did not bat,
    'ret'=retired, 'hit'=hit wicket, 'ob'=obstructing the field
    """
    mapping = {
        "b":        "bowled",
        "ct":       "caught",
        "no":       "not out",
        "not out":  "not out",
        "lbw":      "lbw",
        "ro":       "run out",
        "run out":  "run out",
        "st":       "stumped",
        "dnb":      "did not bat",
        "ret":      "retired",
        "hit":      "hit wicket",
        "ob":       "obstructing the field",
    }
    return mapping.get(str(code).strip().lower(), str(code))


def is_completed(match: dict) -> bool:
    """Return True if the match has a result (not a future fixture)."""
    return bool(match.get("result", "").strip())


# ---------------------------------------------------------------------------
# Step 1 — Ask which season to process
# ---------------------------------------------------------------------------
def ask_season() -> str:
    from datetime import datetime
    current_year = str(datetime.now().year)
    print()
    season = input(f"  Which season year to process? (press Enter for {current_year}): ").strip()
    return season if season else current_year


# ---------------------------------------------------------------------------
# Step 2 — Fetch match summaries
# ---------------------------------------------------------------------------
def fetch_match_summaries(season: str) -> list:
    url    = f"{config.PLAY_CRICKET_BASE_URL}/matches.json"
    params = {
        "api_token": config.PLAY_CRICKET_API_KEY,
        "site_id":   config.PLAY_CRICKET_SITE_ID,
        "season":    season,
    }

    print()
    print(f"  Calling Match Summary API for season {season} ...")
    print(f"  URL : {url}")
    print()

    response = requests.get(url, params=params, timeout=30)
    print(f"  HTTP Status : {response.status_code}")

    if response.status_code != 200:
        print(f"  [ERROR] API call failed. Response: {response.text[:500]}")
        raise SystemExit(1)

    data    = response.json()
    matches = data.get("matches", [])

    if not matches:
        print()
        print(f"  [WARNING] No matches found for season {season}.")
        print(f"  Full response: {data}")
        raise SystemExit(1)

    print(f"  Found {len(matches)} matches in season {season}")
    return matches


# ---------------------------------------------------------------------------
# Step 3 — Preview match list
# ---------------------------------------------------------------------------
def preview_summaries(matches: list):
    print()
    print(f"  {'=' * 70}")
    print(f"  Match list")
    print(f"  {'=' * 70}")
    print(f"  {'#':<4}  {'Match ID':<10}  {'Date':<12}  {'Home':<22}  {'Away'}")
    print(f"  {'-' * 70}")
    for i, m in enumerate(matches, 1):
        match_id  = str(m.get("id", "—"))
        date      = m.get("match_date", "—")
        home      = f"{m.get('home_club_name', '')} {m.get('home_team_name', '')}".strip()
        away      = f"{m.get('away_club_name', '')} {m.get('away_team_name', '')}".strip()
        print(f"  {i:<4}  {match_id:<10}  {date:<12}  {home:<22}  {away}")
    print(f"  {'=' * 70}")


# ---------------------------------------------------------------------------
# Step 4 — Fetch full match detail for each match ID
# ---------------------------------------------------------------------------
def fetch_match_detail(match_id: str) -> dict | None:
    url    = f"{config.PLAY_CRICKET_BASE_URL}/match_detail.json"
    params = {
        "api_token": config.PLAY_CRICKET_API_KEY,
        "match_id":  match_id,
    }
    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        print(f"  [WARN] match {match_id} returned HTTP {response.status_code} — skipping")
        return None

    data = response.json()
    # Response is {"match_details": [ { ...one match... } ]}
    details = data.get("match_details", [])
    return details[0] if details else None


# ---------------------------------------------------------------------------
# Step 5 — Transform match detail into Firestore document shapes
# ---------------------------------------------------------------------------
def transform_match(detail: dict, season_year: str) -> dict:
    home_team_id = str(detail.get("home_team_id", ""))
    our_site_id  = str(config.PLAY_CRICKET_SITE_ID)

    # Determine match_type from competition_type field in detail
    comp_type  = str(detail.get("competition_type", "")).lower()
    if comp_type == "league":
        match_type = "league"
    elif comp_type == "friendly":
        match_type = "friendly"
    else:
        match_type = comp_type  # cup, declaration, etc.

    return {
        "match_id":           str(detail.get("id", "")),
        "site_id":            our_site_id,
        "season_year":        safe_int(season_year),
        "match_type":         match_type,
        "competition_name":   detail.get("competition_name", ""),
        "competition_id":     str(detail.get("competition_id", "")),
        "league_name":        detail.get("league_name", ""),
        "league_id":          str(detail.get("league_id", "")),
        "division":           detail.get("competition_name", ""),
        "date":               parse_date(detail.get("match_date", "")),
        "match_time":         detail.get("match_time", ""),
        "venue":              detail.get("ground_name", ""),
        "ground_id":          str(detail.get("ground_id", "")),
        "home_team": {
            "id":        home_team_id,
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
    # Batting — field is 'bat' in the API
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

    # Bowling — field is 'bowl' in the API, runs conceded is 'runs'
    bowling_perfs = []
    for bw in innings_data.get("bowl", []):
        runs_conceded = safe_int(bw.get("runs", 0))    # note: 'runs' not 'runs_conceded'
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

    # Fall of wickets
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
# Step 6 — Print match detail for inspection
# ---------------------------------------------------------------------------
def preview_detail(detail: dict):
    print(f"    Competition  : {detail.get('competition_name', '—')}  ({detail.get('competition_type', '—')})")
    print(f"    Date         : {detail.get('match_date', '—')}")
    print(f"    Home         : {detail.get('home_club_name', '')} – {detail.get('home_team_name', '')}")
    print(f"    Away         : {detail.get('away_club_name', '')} – {detail.get('away_team_name', '')}")
    print(f"    Result       : {detail.get('result', '—')}  —  {detail.get('result_description', '—')}")
    innings_list = detail.get("innings", [])
    for inn in innings_list:
        print(f"    Innings {inn.get('innings_number', '?')}    : {inn.get('team_batting_name', '—')}  "
              f"{inn.get('runs', '—')}/{inn.get('wickets', '—')}  ({inn.get('overs', '—')} overs)")


# ---------------------------------------------------------------------------
# Step 7 — Write to Firestore
# ---------------------------------------------------------------------------
def write_match(db, detail: dict, season_year: str) -> str:
    match_id  = str(detail.get("id", ""))
    match_doc = transform_match(detail, season_year)

    # Write top-level match document
    db.collection("matches").document(match_id).set(match_doc)

    # Write innings subcollection
    for idx, raw_innings in enumerate(detail.get("innings", []), 1):
        innings_doc = transform_innings(raw_innings)
        innings_num = str(idx)   # use array position (1, 2) not innings_number field
        (
            db.collection("matches")
              .document(match_id)
              .collection("innings")
              .document(innings_num)
              .set(innings_doc)
        )

    return match_id


# ---------------------------------------------------------------------------
# Step 8 — Final summary
# ---------------------------------------------------------------------------
def print_summary(written: list, skipped_future: list, skipped_error: list):
    print()
    print(f"  {'=' * 60}")
    print(f"  Summary")
    print(f"  {'=' * 60}")
    print(f"  Written          : {len(written)} match(es)")
    print(f"  Skipped (future) : {len(skipped_future)}  (no result yet)")
    print(f"  Skipped (error)  : {len(skipped_error)}  (API or data issue)")
    print(f"  {'=' * 60}")
    print()
    print("  ✓  Done. Check the Firestore console to verify:")
    print(f"     https://console.cloud.google.com/firestore/databases/{config.FIRESTORE_DATABASE}/data/matches")
    print()
    print("  Next step: run 04_season_stats.py to compute and write player stats.")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print()
    print("  03_matches.py  —  Fetch matches and innings, write to Firestore")
    print()

    season = ask_season()

    # Fetch and preview match summaries
    summaries = fetch_match_summaries(season)
    preview_summaries(summaries)

    print()
    while True:
        answer = input("  Fetch full details for these matches? (y/n): ").strip().lower()
        if answer in ("y", "yes"):
            break
        if answer in ("n", "no"):
            print("\n  Cancelled — nothing was written.\n")
            return

    # Connect to Firestore
    print()
    print(f"  Connecting to Firestore  (database: {config.FIRESTORE_DATABASE}) ...")
    db = firestore.Client(
        project=config.GCP_PROJECT_ID,
        database=config.FIRESTORE_DATABASE,
    )

    written        = []
    skipped_future = []
    skipped_error  = []

    print()
    for i, summary in enumerate(summaries, 1):
        match_id = str(summary.get("id", ""))
        print(f"  [{i}/{len(summaries)}] Match {match_id}  ({summary.get('match_date', '—')})")

        # Fetch full detail
        time.sleep(API_DELAY_SECONDS)
        detail = fetch_match_detail(match_id)

        if not detail:
            print(f"    [SKIP] No detail returned")
            skipped_error.append(match_id)
            continue

        # Skip future fixtures with no result
        if not is_completed(detail):
            print(f"    [SKIP] No result yet — future fixture")
            skipped_future.append(match_id)
            continue

        # Preview and confirm each match individually
        preview_detail(detail)
        print()
        while True:
            answer = input(f"    Write match {match_id} to Firestore? (y/n/q to quit): ").strip().lower()
            if answer in ("y", "yes"):
                write_match(db, detail, season)
                print(f"    ✓  Written  matches/{match_id}  + innings subcollection")
                written.append(match_id)
                break
            if answer in ("n", "no"):
                print(f"    Skipped.")
                skipped_error.append(match_id)
                break
            if answer in ("q", "quit"):
                print("\n  Stopped early by user.\n")
                print_summary(written, skipped_future, skipped_error)
                return
            print("    Please enter y, n or q.")
        print()

    print_summary(written, skipped_future, skipped_error)


if __name__ == "__main__":
    main()
