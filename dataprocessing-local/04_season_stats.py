"""
04_season_stats.py
Reads all match innings already stored in Firestore and computes
season_stats/{year} documents for every Poplars CC player.

No API calls — works entirely from data already in Firestore.

Run with:
    python 04_season_stats.py

What it does:
  1. Asks which season year to process
  2. Reads all matches for that season from Firestore
  3. For each match reads both innings subcollections
  4. Identifies our batting innings and our bowling innings by team_id
  5. Accumulates batting, bowling and fielding stats per player
  6. Shows a preview table of computed stats
  7. Asks for confirmation then writes season_stats/{year} under each player

Stats logic:
  - Batting stats come from the innings where OUR team batted
  - Bowling stats come from the innings where the OPPONENT batted (we bowled)
  - Fielding (catches/stumpings/run-outs) credited from opponent innings
    batting dismissals where fielder_id matches one of our players
"""

from google.cloud import firestore
import config

# ---------------------------------------------------------------------------
# Step 0 — Validate config
# ---------------------------------------------------------------------------
config.validate()

if not hasattr(config, "OUR_TEAM_ID") or not config.OUR_TEAM_ID:
    print()
    print("  [ERROR] OUR_TEAM_ID is not set in config.py")
    print("  Add this line to config.py:")
    print('  OUR_TEAM_ID = "52930"   # Poplars CC team_id')
    raise SystemExit(1)


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
    """Convert cricket overs notation to balls. 8.4 = 8 overs + 4 balls = 52 balls."""
    whole   = int(overs)
    partial = round((overs - whole) * 10)
    return whole * 6 + partial


def balls_to_overs(balls: int) -> float:
    """Convert balls back to cricket overs notation. 52 balls = 8.4 overs."""
    return float(f"{balls // 6}.{balls % 6}")


def safe_divide(num: float, den: float) -> float:
    return round(num / den, 2) if den else 0.0


def is_better_bowling(new_w: int, new_r: int, best_w: int, best_r: int) -> bool:
    if new_w > best_w:
        return True
    if new_w == best_w and new_r < best_r:
        return True
    return False


def empty_stats() -> dict:
    return {
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
# Step 1 — Ask which season
# ---------------------------------------------------------------------------
def ask_season() -> str:
    from datetime import datetime
    current_year = str(datetime.now().year)
    print()
    season = input(f"  Which season year to compute stats for? (press Enter for {current_year}): ").strip()
    return season if season else current_year


# ---------------------------------------------------------------------------
# Step 2 — Load all matches + innings for the season from Firestore
# ---------------------------------------------------------------------------
def load_innings_for_season(db, season_year: int) -> list:
    """
    Returns a list of (match_id, innings_dict) tuples for the given season.
    Reads matches filtered by season_year, then reads each innings subcollection.
    """
    print()
    print(f"  Reading matches for season {season_year} from Firestore ...")

    matches_ref = (
        db.collection("matches")
          .where("season_year", "==", season_year)
          .stream()
    )

    match_docs = list(matches_ref)
    print(f"  Found {len(match_docs)} match(es)")

    if not match_docs:
        print()
        print(f"  [WARNING] No matches found for season {season_year}.")
        print(f"  Make sure 03_matches.py has been run for this season first.")
        raise SystemExit(1)

    all_innings = []
    for match_doc in match_docs:
        match_id = match_doc.id
        innings_ref = (
            db.collection("matches")
              .document(match_id)
              .collection("innings")
              .stream()
        )
        for inn in innings_ref:
            all_innings.append((match_id, inn.to_dict()))

    print(f"  Found {len(all_innings)} innings document(s) across all matches")
    return all_innings


# ---------------------------------------------------------------------------
# Step 3 — Compute stats from innings data
# ---------------------------------------------------------------------------
def compute_stats(all_innings: list, season_year: int) -> dict:
    """
    Returns {player_id: stats_dict} for Poplars CC players only.
    """
    our_team_id     = str(config.OUR_TEAM_ID)
    player_stats:   dict[str, dict] = {}
    player_matches: dict[str, set]  = {}

    # Build a set of our player IDs from all our batting innings
    # so we can filter fielding credits correctly
    our_player_ids: set = set()
    for match_id, innings in all_innings:
        if str(innings.get("team_id", "")) == our_team_id:
            for b in innings.get("batting_performances", []):
                if b.get("player_id"):
                    our_player_ids.add(str(b.get("player_id")))
        else:
            for bw in innings.get("bowling_performances", []):
                if bw.get("player_id"):
                    our_player_ids.add(str(bw.get("player_id")))

    for match_id, innings in all_innings:
        team_id = str(innings.get("team_id", ""))
        is_our_batting_innings   = team_id == our_team_id
        is_our_bowling_innings   = team_id != our_team_id

        # ---- Batting -------------------------------------------------------
        if is_our_batting_innings:
            for b in innings.get("batting_performances", []):
                pid        = str(b.get("player_id", ""))
                dismissal  = b.get("dismissal_type", "").lower()

                if not pid:
                    continue
                if dismissal == "did not bat":
                    continue

                if pid not in player_stats:
                    player_stats[pid] = empty_stats()
                player_matches.setdefault(pid, set()).add(match_id)

                runs = safe_int(b.get("runs", 0))
                player_stats[pid]["batting"]["innings"] += 1
                player_stats[pid]["batting"]["runs"]    += runs

                if dismissal == "not out":
                    player_stats[pid]["batting"]["not_outs"] += 1
                elif runs == 0:
                    player_stats[pid]["batting"]["ducks"] += 1

                if runs > player_stats[pid]["batting"]["highest_score"]:
                    player_stats[pid]["batting"]["highest_score"] = runs
                if runs >= 100:
                    player_stats[pid]["batting"]["hundreds"] += 1
                elif runs >= 50:
                    player_stats[pid]["batting"]["fifties"] += 1

        # ---- Bowling -------------------------------------------------------
        if is_our_bowling_innings:
            for bw in innings.get("bowling_performances", []):
                pid = str(bw.get("player_id", ""))
                if not pid or pid not in our_player_ids:
                    continue

                if pid not in player_stats:
                    player_stats[pid] = empty_stats()
                player_matches.setdefault(pid, set()).add(match_id)

                overs         = safe_float(bw.get("overs", 0))
                runs_conceded = safe_int(bw.get("runs_conceded", 0))
                wickets       = safe_int(bw.get("wickets", 0))

                player_stats[pid]["bowling"]["_balls"]        += overs_to_balls(overs)
                player_stats[pid]["bowling"]["maidens"]       += safe_int(bw.get("maidens", 0))
                player_stats[pid]["bowling"]["runs_conceded"] += runs_conceded
                player_stats[pid]["bowling"]["wickets"]       += wickets

                if wickets >= 5:
                    player_stats[pid]["bowling"]["five_wickets"] += 1

                best_str = player_stats[pid]["bowling"]["best_bowling"]
                try:
                    best_w, best_r = map(int, best_str.split("/"))
                except Exception:
                    best_w, best_r = 0, 9999

                if is_better_bowling(wickets, runs_conceded, best_w, best_r):
                    player_stats[pid]["bowling"]["best_bowling"] = f"{wickets}/{runs_conceded}"

        # ---- Fielding ------------------------------------------------------
        # Credits come from the OPPONENT batting innings dismissal records
        if is_our_bowling_innings:
            for b in innings.get("batting_performances", []):
                fielder_id = str(b.get("fielder_id", ""))
                dismissal  = b.get("dismissal_type", "").lower()

                if not fielder_id or fielder_id not in our_player_ids:
                    continue

                if fielder_id not in player_stats:
                    player_stats[fielder_id] = empty_stats()
                player_matches.setdefault(fielder_id, set()).add(match_id)

                if dismissal == "caught":
                    player_stats[fielder_id]["fielding"]["catches"]   += 1
                elif dismissal == "stumped":
                    player_stats[fielder_id]["fielding"]["stumpings"] += 1
                elif "run out" in dismissal:
                    player_stats[fielder_id]["fielding"]["run_outs"]  += 1

    # ---- Derived fields and matches played ---------------------------------
    for pid, stats in player_stats.items():
        stats["matches_played"] = len(player_matches.get(pid, set()))
        stats["year"]           = season_year
        stats["team_id"]        = our_team_id

        # Batting average
        b          = stats["batting"]
        dismissals = b["innings"] - b["not_outs"]
        b["average"] = safe_divide(b["runs"], dismissals)

        # Bowling — convert balls to overs, compute average and economy
        bw = stats["bowling"]
        bw["overs"]   = balls_to_overs(bw.pop("_balls", 0))
        bw["average"] = safe_divide(bw["runs_conceded"], bw["wickets"])
        bw["economy"] = safe_divide(bw["runs_conceded"], bw["overs"])

    return player_stats


# ---------------------------------------------------------------------------
# Step 4 — Load player names for the preview table
# ---------------------------------------------------------------------------
def load_player_names(db, player_ids: set) -> dict:
    """Returns {player_id: display_name} from the players/ collection."""
    names = {}
    for pid in player_ids:
        doc = db.collection("players").document(pid).get()
        if doc.exists:
            data = doc.to_dict()
            names[pid] = data.get("name", pid)
        else:
            names[pid] = pid   # fallback to ID if not found
    return names


# ---------------------------------------------------------------------------
# Step 5 — Preview computed stats
# ---------------------------------------------------------------------------
def preview_stats(player_stats: dict, player_names: dict):
    print()
    print(f"  {'=' * 95}")
    print(f"  Computed season stats for {len(player_stats)} player(s)")
    print(f"  {'=' * 95}")
    print(f"  {'Player':<25}  {'M':>3}  {'Runs':>5}  {'Avg':>6}  {'HS':>4}  "
          f"{'50s':>3}  {'100s':>4}  {'Wkts':>5}  {'BBF':>6}  {'Econ':>5}  "
          f"{'Ct':>3}  {'St':>3}  {'RO':>3}")
    print(f"  {'-' * 95}")
    for pid, s in sorted(player_stats.items(),
                         key=lambda x: player_names.get(x[0], "")):
        name = player_names.get(pid, pid)[:24]
        b    = s["batting"]
        bw   = s["bowling"]
        f    = s["fielding"]
        print(
            f"  {name:<25}  {s['matches_played']:>3}  "
            f"{b['runs']:>5}  {b['average']:>6.2f}  {b['highest_score']:>4}  "
            f"{b['fifties']:>3}  {b['hundreds']:>4}  "
            f"{bw['wickets']:>5}  {bw['best_bowling']:>6}  {bw['economy']:>5.2f}  "
            f"{f['catches']:>3}  {f['stumpings']:>3}  {f['run_outs']:>3}"
        )
    print(f"  {'=' * 95}")


# ---------------------------------------------------------------------------
# Step 6 — Confirm and write to Firestore
# ---------------------------------------------------------------------------
def confirm_write() -> bool:
    print()
    while True:
        answer = input("  Write these season stats to Firestore? (y/n): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("  Please enter y or n.")


def write_stats(db, player_stats: dict):
    print()
    for pid, stats in player_stats.items():
        year = stats.get("year")
        stats["last_updated"] = firestore.SERVER_TIMESTAMP
        (
            db.collection("players")
              .document(pid)
              .collection("season_stats")
              .document(str(year))
              .set(stats, merge=False)
        )
        print(f"  ✓  Written  players/{pid}/season_stats/{year}")


# ---------------------------------------------------------------------------
# Step 7 — Summary
# ---------------------------------------------------------------------------
def print_summary(player_stats: dict, season_year: int):
    print()
    print(f"  {'=' * 60}")
    print(f"  Summary")
    print(f"  {'=' * 60}")
    print(f"  Season        : {season_year}")
    print(f"  Players written : {len(player_stats)}")
    print(f"  {'=' * 60}")
    print()
    print("  ✓  Done. Check the Firestore console to verify:")
    print(f"     https://console.cloud.google.com/firestore/databases/{config.FIRESTORE_DATABASE}/data/players")
    print()
    print("  Next step: run 05_career_stats.py to aggregate career totals.")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print()
    print("  04_season_stats.py  —  Compute and write player season stats")
    print(f"  Computing for Poplars CC  (team_id: {config.OUR_TEAM_ID})")
    print()

    season_year = int(ask_season())

    db = firestore.Client(
        project=config.GCP_PROJECT_ID,
        database=config.FIRESTORE_DATABASE,
    )

    all_innings  = load_innings_for_season(db, season_year)
    player_stats = compute_stats(all_innings, season_year)

    if not player_stats:
        print()
        print("  [WARNING] No player stats computed — check that innings documents")
        print("  contain batting_performances and bowling_performances arrays.")
        return

    player_names = load_player_names(db, set(player_stats.keys()))
    preview_stats(player_stats, player_names)

    confirmed = confirm_write()
    if not confirmed:
        print("\n  Cancelled — nothing was written.\n")
        return

    write_stats(db, player_stats)
    print_summary(player_stats, season_year)


if __name__ == "__main__":
    main()
