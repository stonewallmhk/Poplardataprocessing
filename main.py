"""
main.py
Entry point for the Poplars historical backfill Cloud Run Job.

Execution order (matches the design document Section 5.2):
  Step 1  — Fetch and upsert all teams
  Step 2  — Fetch and upsert all players
  Step 3  — For each year, for each team:
               fetch match summaries → fetch match details → write to Firestore
               then compute and write season stats from in-memory innings data
  Step 4  — Compute and write career stats for every player that appeared

Environment variables required:
  GOOGLE_CLOUD_PROJECT    — set automatically by Cloud Run
  PLAY_CRICKET_SITE_ID    — your Play-Cricket site_id (set in Cloud Run job config)

Secret Manager secret required:
  playcricket-api-key     — your Play-Cricket API token
"""

import os
import time
import logging
from datetime import datetime
from google.cloud import firestore, secretmanager

from api_client      import PlayCricketAPI
from firestore_writer import FirestoreWriter
from stats           import build_season_stats, write_season_stats, write_career_stats

# ---------------------------------------------------------------------------
# Logging — plain format works well in Cloud Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID   = os.environ.get("GOOGLE_CLOUD_PROJECT")
DATABASE     = "poplarsdb"
SITE_ID      = os.environ.get("PLAY_CRICKET_SITE_ID")
CURRENT_YEAR = datetime.now().year
BACKFILL_YEARS = list(range(CURRENT_YEAR - 4, CURRENT_YEAR + 1))  # last 5 seasons

# Delay between Match Details API calls to respect Play-Cricket rate limits
API_CALL_DELAY_SECONDS = 0.5


# ---------------------------------------------------------------------------
# Secret Manager helper
# ---------------------------------------------------------------------------
def get_api_key() -> str:
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{PROJECT_ID}/secrets/playcricket-api-key/versions/latest"
    resp   = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("UTF-8").strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("Poplars Backfill  —  starting")
    log.info(f"Project : {PROJECT_ID}")
    log.info(f"Database: {DATABASE}")
    log.info(f"Site ID : {SITE_ID}")
    log.info(f"Years   : {BACKFILL_YEARS}")
    log.info("=" * 60)

    if not SITE_ID:
        raise RuntimeError("PLAY_CRICKET_SITE_ID environment variable is not set.")

    # Initialise clients
    api_key = get_api_key()
    api     = PlayCricketAPI(api_key, SITE_ID)
    db      = firestore.Client(project=PROJECT_ID, database=DATABASE)
    writer  = FirestoreWriter(db)

    # ------------------------------------------------------------------
    # Step 1 — Teams
    # ------------------------------------------------------------------
    log.info("--- Step 1: Teams ---")
    teams = api.get_teams()
    for team in teams:
        writer.upsert_team(team)
    log.info(f"Wrote {len(teams)} team(s)")

    # Build a lookup of team_id strings for use below
    team_ids = [str(t.get("id", "")) for t in teams]

    # ------------------------------------------------------------------
    # Step 2 — Players
    # ------------------------------------------------------------------
    log.info("--- Step 2: Players ---")
    players = api.get_players()
    for player in players:
        writer.upsert_player(player)
    log.info(f"Wrote {len(players)} player(s)")

    # Track every player_id seen across all seasons for career stats
    all_player_ids: set = set()

    # ------------------------------------------------------------------
    # Step 3 — Matches, year by year
    # ------------------------------------------------------------------
    for year in BACKFILL_YEARS:
        log.info(f"--- Step 3: Season {year} ---")

        league_match_ids:   list = []
        friendly_match_ids: list = []
        all_innings_this_year:  list = []   # (match_id, innings_dict) tuples

        for team_id in team_ids:
            summaries = api.get_match_summary(year, team_id)

            for summary in summaries:
                match_id = str(summary.get("id", ""))
                if not match_id:
                    continue

                # Determine match type from the summary to track match IDs
                competition_type = summary.get("competition_type", "")
                is_league = competition_type == "L"

                # Skip if already in Firestore with a result (idempotency)
                if writer.match_already_complete(match_id):
                    log.info(f"  Skipping {match_id} — already complete")
                    if is_league:
                        league_match_ids.append(match_id)
                    else:
                        friendly_match_ids.append(match_id)
                    continue

                # Fetch full match details
                time.sleep(API_CALL_DELAY_SECONDS)
                detail = api.get_match_details(match_id)

                if not detail:
                    log.warning(f"  No detail returned for match {match_id} — skipping")
                    continue

                # Skip matches with no result yet (future fixtures)
                if not detail.get("result"):
                    log.info(f"  Skipping {match_id} — no result yet (future fixture?)")
                    continue

                # Write match + innings, collect innings for stats
                innings_tuples = writer.write_match(detail, team_id, year)
                all_innings_this_year.extend(innings_tuples)

                # Track match ID in the correct list
                if is_league:
                    league_match_ids.append(match_id)
                else:
                    friendly_match_ids.append(match_id)

                log.info(f"  Written match {match_id}  "
                         f"({'league' if is_league else 'friendly'})")

        # Compute season stats from in-memory innings data (no extra Firestore reads)
        if all_innings_this_year:
            log.info(f"  Computing season stats for {year} ...")
            player_stats = build_season_stats(year, all_innings_this_year)
            write_season_stats(db, player_stats)
            all_player_ids.update(player_stats.keys())

        # Write / update the season summary document
        writer.upsert_season(year, league_match_ids, friendly_match_ids)

        log.info(
            f"Season {year} complete — "
            f"{len(league_match_ids)} league, {len(friendly_match_ids)} friendly"
        )

    # ------------------------------------------------------------------
    # Step 4 — Career stats
    # ------------------------------------------------------------------
    log.info("--- Step 4: Career stats ---")
    write_career_stats(db, all_player_ids)

    log.info("=" * 60)
    log.info("Backfill complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
