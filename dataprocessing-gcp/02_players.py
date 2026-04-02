"""
02_players.py
GCP-ready player sync that fetches players from the Play-Cricket API and
upserts them into Firestore.

Designed to run in two modes:

  GCP (Cloud Function):
    HTTP entry point: sync_players(request)
    Optional query param: ?include_historic=yes|no  (defaults to yes)

  Local testing:
    python 02_players.py
    python 02_players.py --no-historic
"""

import logging
import sys

import requests
from google.cloud import firestore

import config

config.setup_logging()
log = logging.getLogger(__name__)


def split_name(full_name: str) -> tuple[str, str]:
    """Split a display name into first and last name parts."""
    parts = full_name.strip().split(" ", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""


def fetch_players(api_key: str, include_historic: bool = True) -> list:
    """Fetch all current or historic players from Play-Cricket."""
    url = f"{config.PLAY_CRICKET_BASE_URL}/sites/{config.PLAY_CRICKET_SITE_ID}/players"
    params = {
        "api_token": api_key,
        "include_historic": "yes" if include_historic else "no",
    }

    log.info("Calling Players API (include_historic=%s)", params["include_historic"])
    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        log.error("Players API returned HTTP %s: %s", response.status_code, response.text[:300])
        return []

    players = response.json().get("players", [])
    log.info("Players API returned %s player(s)", len(players))
    return players


def transform_player(player: dict) -> dict | None:
    """Transform one API player object into the Firestore document shape."""
    member_id = str(player.get("member_id", "")).strip()
    full_name = player.get("name", "").strip()

    if not member_id:
        return None

    first_name, last_name = split_name(full_name)
    return {
        "member_id": member_id,
        "site_id": str(config.PLAY_CRICKET_SITE_ID),
        "name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "is_active": True,
        "last_updated": firestore.SERVER_TIMESTAMP,
    }


def write_players(db, players: list) -> tuple[int, int]:
    """Upsert player documents into Firestore."""
    written = 0
    skipped = 0

    for raw_player in players:
        player_doc = transform_player(raw_player)
        if not player_doc:
            skipped += 1
            log.warning("Skipping player with no member_id: %s", raw_player)
            continue

        member_id = player_doc["member_id"]
        db.collection("players").document(member_id).set(player_doc, merge=True)
        written += 1

    log.info("Upserted %s player(s); skipped %s", written, skipped)
    return written, skipped


def run_player_sync(include_historic: bool = True) -> dict:
    """Fetch players from Play-Cricket and write them to Firestore."""
    log.info("%s", "=" * 55)
    log.info("Starting player sync")
    log.info("%s", "=" * 55)

    api_key = config.get_api_key()
    db = config.get_db()

    players = fetch_players(api_key, include_historic=include_historic)
    if not players:
        log.warning("No players returned by API")
        return {
            "written": 0,
            "skipped": 0,
            "include_historic": include_historic,
        }

    written, skipped = write_players(db, players)
    summary = {
        "written": written,
        "skipped": skipped,
        "include_historic": include_historic,
    }

    log.info("Player sync complete: %s written, %s skipped", written, skipped)
    return summary


try:
    import functions_framework

    @functions_framework.http
    def sync_players(request):
        """HTTP-triggered Cloud Function entry point for player sync."""
        include_historic_arg = (request.args.get("include_historic", "yes") if request else "yes").strip().lower()
        include_historic = include_historic_arg not in {"0", "false", "no"}

        result = run_player_sync(include_historic=include_historic)
        return (
            "Player sync complete - "
            f"written: {result['written']}, skipped: {result['skipped']}, "
            f"include_historic: {str(result['include_historic']).lower()}",
            200,
        )

except ImportError:
    pass


if __name__ == "__main__":
    include_historic = "--no-historic" not in sys.argv[1:]
    result = run_player_sync(include_historic=include_historic)
    print()
    print(
        f"Player sync complete - written: {result['written']}, "
        f"skipped: {result['skipped']}, include_historic: {str(result['include_historic']).lower()}"
    )
    print()
