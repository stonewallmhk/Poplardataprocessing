"""
04_sync.py
Match sync orchestrator — fetches new matches from Play-Cricket, writes them
to Firestore, then incrementally updates player season and career stats.

Designed to run in two modes:

  GCP (Cloud Function):
    Deployed as an HTTP-triggered Cloud Function.
    Triggered weekly by Cloud Scheduler.
    Entry point: sync(request)
    Optional query param: ?season_year=2023  (defaults to current year)

  Local testing:
    python 04_sync.py                      # current season
    python 04_sync.py 2023                 # specific historical season
    python 04_sync.py 2020 2021 2022 2023  # multiple seasons

GCP deployment command (run from this folder):
  gcloud functions deploy poplars-sync ^
    --gen2 ^
    --runtime python311 ^
    --region europe-west2 ^
    --source . ^
    --entry-point sync ^
    --trigger-http ^
    --no-allow-unauthenticated ^
    --service-account poplars-pipeline@YOUR_PROJECT_ID.iam.gserviceaccount.com ^
    --set-env-vars PLAY_CRICKET_SITE_ID=YOUR_SITE_ID ^
    --timeout 300s

Cloud Scheduler setup (after deploying the function):
  gcloud scheduler jobs create http poplars-weekly-sync ^
    --schedule "0 20 * * 0" ^
    --uri "https://europe-west2-YOUR_PROJECT_ID.cloudfunctions.net/poplars-sync" ^
    --http-method GET ^
    --oidc-service-account-email poplars-pipeline@YOUR_PROJECT_ID.iam.gserviceaccount.com ^
    --location europe-west2

  The cron "0 20 * * 0" fires every Sunday at 20:00 UTC (9pm BST in summer).
"""

import sys
import time
import logging
from datetime import datetime

import config
import api_helpers
import stats_engine

config.setup_logging()
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core sync logic — shared between GCP and local modes
# ---------------------------------------------------------------------------

def run_sync(season_year: int) -> dict:
    """
    Sync all new completed matches for a given season year.

    1. Fetches match summaries from Play-Cricket API
    2. Compares against existing Firestore match IDs (diff approach)
    3. For each new completed match:
       a. Fetches full match detail
       b. Writes match + innings to Firestore
       c. Updates season_stats for affected players
       d. Updates career_stats for affected players
    4. Returns a summary dict for logging

    The diff approach makes this fully idempotent — running it twice
    in the same week produces no duplicate data.
    """
    log.info(f"{'=' * 55}")
    log.info(f"Starting sync for season {season_year}")
    log.info(f"{'=' * 55}")

    api_key = config.get_api_key()
    db      = config.get_db()

    # Step 1 — Fetch match summaries from API
    summaries = api_helpers.fetch_match_summaries(api_key, str(season_year))
    if not summaries:
        log.warning(f"No matches returned by API for season {season_year}")
        return {"season": season_year, "processed": 0, "skipped": 0, "errors": 0}

    api_match_ids = {str(m.get("id", "")) for m in summaries if m.get("id")}
    log.info(f"API returned {len(api_match_ids)} match IDs for {season_year}")

    # Step 2 — Get existing Firestore match IDs for this season
    existing_ids = api_helpers.get_existing_match_ids(db, season_year)
    log.info(f"Firestore already has {len(existing_ids)} matches for {season_year}")

    # Step 3 — Compute diff
    new_match_ids = api_match_ids - existing_ids
    log.info(f"New matches to process: {len(new_match_ids)}")

    if not new_match_ids:
        log.info("Nothing to do — Firestore is up to date")
        return {"season": season_year, "processed": 0, "skipped": 0, "errors": 0}

    # Step 4 — Process each new match
    processed = 0
    skipped   = 0
    errors    = 0
    all_updated_players: set = set()

    for match_id in sorted(new_match_ids):
        log.info(f"Processing match {match_id} ...")
        time.sleep(config.API_CALL_DELAY_SECONDS)

        try:
            # Fetch full match detail
            detail = api_helpers.fetch_match_detail(api_key, match_id)

            if not detail:
                log.warning(f"  No detail returned for match {match_id} — skipping")
                errors += 1
                continue

            # Skip future fixtures (no result yet)
            if not api_helpers.is_completed(detail):
                log.info(f"  Match {match_id} has no result yet — future fixture, skipping")
                skipped += 1
                continue

            # Write match + innings to Firestore
            _, innings_list = api_helpers.write_match_to_firestore(db, detail, season_year)
            log.info(f"  Written match {match_id} with {len(innings_list)} innings")

            # Update season stats incrementally
            updated_pids = stats_engine.update_season_stats(db, innings_list, season_year)
            log.info(f"  Updated season_stats for {len(updated_pids)} player(s)")
            all_updated_players.update(updated_pids)

            processed += 1

        except Exception as e:
            log.error(f"  Error processing match {match_id}: {e}", exc_info=True)
            errors += 1
            continue

    # Step 5 — Update career stats for all affected players
    if all_updated_players:
        log.info(f"Updating career_stats for {len(all_updated_players)} player(s) ...")
        stats_engine.update_career_stats(db, all_updated_players)

    summary = {
        "season":    season_year,
        "processed": processed,
        "skipped":   skipped,
        "errors":    errors,
        "players_updated": len(all_updated_players),
    }

    log.info(f"{'=' * 55}")
    log.info(f"Sync complete for {season_year}")
    log.info(f"  Processed : {processed}")
    log.info(f"  Skipped   : {skipped}  (future fixtures)")
    log.info(f"  Errors    : {errors}")
    log.info(f"  Players   : {len(all_updated_players)} updated")
    log.info(f"{'=' * 55}")

    return summary


# ---------------------------------------------------------------------------
# GCP Cloud Function entry point
# Decorated with @functions_framework.http for HTTP trigger.
# Cloud Scheduler calls this via GET with optional ?season_year= param.
# ---------------------------------------------------------------------------

try:
    import functions_framework

    @functions_framework.http
    def sync(request):
        """
        HTTP-triggered Cloud Function entry point.
        Called by Cloud Scheduler every Sunday evening.
        Optional query param: ?season_year=2023
        """
        season_year = request.args.get("season_year")
        if season_year:
            years = [int(season_year)]
        else:
            years = [datetime.now().year]

        results = []
        for year in years:
            result = run_sync(year)
            results.append(result)

        summary = ", ".join(
            f"{r['season']}: {r['processed']} processed, {r['errors']} errors"
            for r in results
        )
        return f"Sync complete — {summary}", 200

except ImportError:
    # functions_framework not installed locally — that's fine.
    # The GCP entry point is unavailable but local mode works normally.
    pass


# ---------------------------------------------------------------------------
# Local entry point
# python 04_sync.py                      → current season
# python 04_sync.py 2023                 → specific season
# python 04_sync.py 2020 2021 2022 2023  → multiple seasons
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = sys.argv[1:]

    if args:
        years = []
        for a in args:
            try:
                years.append(int(a))
            except ValueError:
                print(f"  [ERROR] Invalid season year: {a}")
                sys.exit(1)
    else:
        years = [datetime.now().year]

    print()
    if len(years) == 1:
        print(f"  Running sync for season {years[0]}")
    else:
        print(f"  Running sync for seasons: {years}")
    print()

    for year in years:
        run_sync(year)
