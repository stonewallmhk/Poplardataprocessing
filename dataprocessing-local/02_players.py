"""
02_players.py
Fetches all players for your club from the Play-Cricket Players API and writes
them to the Firestore players/ collection in poplarsdb.

Run with:
    python 02_players.py

What it does:
  1. Calls GET /api/v2/sites/{site_id}/players?include_historic=yes
  2. Splits the single 'name' field into first_name and last_name
  3. Prints every player returned so you can inspect before writing
  4. Asks for confirmation
  5. Writes each player to players/{member_id} in Firestore
  6. Prints a final summary of what was written

API notes:
  - Returns 'name' as a single combined field (e.g. "Bugs Bunny")
  - include_historic=yes captures past players needed for historical stats
  - No .json suffix on this endpoint
"""

import requests
from google.cloud import firestore
import config

# ---------------------------------------------------------------------------
# Step 0 — Validate config
# ---------------------------------------------------------------------------
config.validate()


# ---------------------------------------------------------------------------
# Name splitting helper
# ---------------------------------------------------------------------------
def split_name(full_name: str) -> tuple[str, str]:
    """
    Split 'Bugs Bunny' into ('Bugs', 'Bunny').
    For names with multiple parts e.g. 'Mary Jane Watson',
    first_name = 'Mary', last_name = 'Jane Watson'.
    For a single word name, last_name will be empty.
    """
    parts = full_name.strip().split(" ", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""


# ---------------------------------------------------------------------------
# Step 1 — Fetch players from Play-Cricket API
# ---------------------------------------------------------------------------
def fetch_players() -> list:
    url    = f"{config.PLAY_CRICKET_BASE_URL}/sites/{config.PLAY_CRICKET_SITE_ID}/players"
    params = {
        "api_token":        config.PLAY_CRICKET_API_KEY,
        "include_historic": "yes",   # includes past players for historical stats
    }

    print()
    print(f"  Calling Players API ...")
    print(f"  URL : {url}")
    print()

    response = requests.get(url, params=params, timeout=30)

    print(f"  HTTP Status : {response.status_code}")

    if response.status_code != 200:
        print()
        print(f"  [ERROR] API call failed.")
        print(f"  Response: {response.text[:500]}")
        raise SystemExit(1)

    data    = response.json()
    players = data.get("players", [])

    if not players:
        print()
        print("  [WARNING] API returned no players.")
        print(f"  Full response: {data}")
        raise SystemExit(1)

    return players


# ---------------------------------------------------------------------------
# Step 2 — Print players for inspection
# ---------------------------------------------------------------------------
def preview_players(players: list):
    print()
    print(f"  {'=' * 60}")
    print(f"  Players returned by API  ({len(players)} total)")
    print(f"  {'=' * 60}")
    print(f"  {'#':<5}  {'Member ID':<12}  {'Full Name':<25}  {'First':<15}  {'Last'}")
    print(f"  {'-' * 60}")
    for i, player in enumerate(players, 1):
        member_id  = str(player.get("member_id", "—"))
        full_name  = player.get("name", "—")
        first, last = split_name(full_name)
        print(f"  {i:<5}  {member_id:<12}  {full_name:<25}  {first:<15}  {last}")
    print(f"  {'=' * 60}")
    print()
    print("  Note: first_name and last_name are split from the single 'name' field.")
    print("  If any splits look wrong, you can correct them manually in Firestore after writing.")


# ---------------------------------------------------------------------------
# Step 3 — Confirm before writing
# ---------------------------------------------------------------------------
def confirm_write() -> bool:
    print()
    while True:
        answer = input("  Write these players to Firestore? (y/n): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("  Please enter y or n.")


# ---------------------------------------------------------------------------
# Step 4 — Write to Firestore
# ---------------------------------------------------------------------------
def write_players(players: list):
    print()
    print(f"  Connecting to Firestore  (database: {config.FIRESTORE_DATABASE}) ...")
    db = firestore.Client(
        project=config.GCP_PROJECT_ID,
        database=config.FIRESTORE_DATABASE,
    )

    written = []
    skipped = []

    for player in players:
        member_id = str(player.get("member_id", "")).strip()
        full_name = player.get("name", "").strip()

        if not member_id:
            print(f"  [SKIP] Player has no member_id — raw data: {player}")
            skipped.append(player)
            continue

        first_name, last_name = split_name(full_name)

        doc = {
            "member_id":        member_id,
            "site_id":          str(config.PLAY_CRICKET_SITE_ID),
            "name":             full_name,       # keep original for reference
            "first_name":       first_name,
            "last_name":        last_name,
            "is_active":        True,
            "last_updated":     firestore.SERVER_TIMESTAMP,
        }

        # merge=True preserves any manually added fields (email, DOB, playing_role etc.)
        db.collection("players").document(member_id).set(doc, merge=True)
        print(f"  ✓  Written  players/{member_id}  →  {full_name}")
        written.append((member_id, full_name))

    return written, skipped


# ---------------------------------------------------------------------------
# Step 5 — Final summary
# ---------------------------------------------------------------------------
def print_summary(written: list, skipped: list):
    print()
    print(f"  {'=' * 60}")
    print(f"  Summary")
    print(f"  {'=' * 60}")
    print(f"  Written  : {len(written)} player(s)")
    if skipped:
        print(f"  Skipped  : {len(skipped)}  (no member_id in API response)")
    print(f"  {'=' * 60}")
    print()
    print("  ✓  Done. Check the Firestore console to verify:")
    print(f"     https://console.cloud.google.com/firestore/databases/{config.FIRESTORE_DATABASE}/data/players")
    print()
    print("  Next step: manually add any extra fields to player documents")
    print("  that the API does not provide (email, date_of_birth, playing_role).")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print()
    print("  02_players.py  —  Fetch players and write to Firestore")
    print()

    players          = fetch_players()
    preview_players(players)
    confirmed        = confirm_write()

    if not confirmed:
        print()
        print("  Cancelled — nothing was written.")
        print()
        return

    written, skipped = write_players(players)
    print_summary(written, skipped)


if __name__ == "__main__":
    main()
