"""
01_teams.py
Fetches all teams for your club from the Play-Cricket Teams API and writes
them to the Firestore teams/ collection in poplarsdb.

Run with:
    python 01_teams.py

What it does:
  1. Calls GET /api/v1/sites/{site_id}/teams
  2. Prints every team returned so you can inspect before writing
  3. Asks for confirmation
  4. Writes each team to teams/{team_id} in Firestore
  5. Prints a final summary of what was written
"""

import requests
from google.cloud import firestore
import config

# ---------------------------------------------------------------------------
# Step 0 — Validate config before doing anything
# ---------------------------------------------------------------------------
config.validate()

# ---------------------------------------------------------------------------
# Step 1 — Fetch teams from Play-Cricket API
# ---------------------------------------------------------------------------
def fetch_teams() -> list:
    url    = f"{config.PLAY_CRICKET_BASE_URL}/sites/{config.PLAY_CRICKET_SITE_ID}/teams.json"
    params = {"api_token": config.PLAY_CRICKET_API_KEY}
    print(f"  Full URL    : {url}")
    print(f"  API Key set : {'Yes' if config.PLAY_CRICKET_API_KEY != 'YOUR_API_KEY' else 'NO - still using fallback!'}")

    print()
    print(f"  Calling Teams API ...")
    print(f"  URL : {url}")
    print()

    response = requests.get(url, params=params, timeout=30)

    # Show the HTTP status clearly
    print(f"  HTTP Status : {response.status_code}")

    if response.status_code != 200:
        print()
        print(f"  [ERROR] API call failed.")
        print(f"  Response body: {response.text[:500]}")
        raise SystemExit(1)

    data  = response.json()
    teams = data.get("teams", [])

    if not teams:
        print()
        print("  [WARNING] API returned no teams.")
        print("  Full response:")
        print(f"  {data}")
        raise SystemExit(1)

    return teams


# ---------------------------------------------------------------------------
# Step 2 — Print teams for inspection
# ---------------------------------------------------------------------------
def preview_teams(teams: list):
    print()
    print(f"  {'=' * 55}")
    print(f"  Teams returned by API  ({len(teams)} total)")
    print(f"  {'=' * 55}")
    print(f"  {'#':<4}  {'Team ID':<12}  {'Team Name'}")
    print(f"  {'-' * 55}")
    for i, team in enumerate(teams, 1):
        team_id   = str(team.get("id", "—"))
        team_name = team.get("name", "—")
        print(f"  {i:<4}  {team_id:<12}  {team_name}")
    print(f"  {'=' * 55}")


# ---------------------------------------------------------------------------
# Step 3 — Confirm before writing
# ---------------------------------------------------------------------------
def confirm_write() -> bool:
    print()
    while True:
        answer = input("  Write these teams to Firestore? (y/n): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("  Please enter y or n.")


# ---------------------------------------------------------------------------
# Step 4 — Write to Firestore
# ---------------------------------------------------------------------------
def write_teams(teams: list):
    print()
    print(f"  Connecting to Firestore  (database: {config.FIRESTORE_DATABASE}) ...")
    db = firestore.Client(
        project=config.GCP_PROJECT_ID,
        database=config.FIRESTORE_DATABASE,
    )

    written  = []
    skipped  = []

    for team in teams:
        team_id   = str(team.get("id", "")).strip()
        team_name = team.get("name", "").strip()

        if not team_id:
            print(f"  [SKIP] Team has no ID — raw data: {team}")
            skipped.append(team)
            continue

        doc = {
            "team_id":    team_id,
            "site_id":    str(config.PLAY_CRICKET_SITE_ID),
            "name":       team_name,
            "is_active":  True,
            "created_at": firestore.SERVER_TIMESTAMP,
        }

        db.collection("teams").document(team_id).set(doc, merge=True)
        print(f"  ✓  Written  teams/{team_id}  →  {team_name}")
        written.append(team_id)

    return written, skipped


# ---------------------------------------------------------------------------
# Step 5 — Final summary
# ---------------------------------------------------------------------------
def print_summary(written: list, skipped: list):
    print()
    print(f"  {'=' * 55}")
    print(f"  Summary")
    print(f"  {'=' * 55}")
    print(f"  Written  : {len(written)}")
    if written:
        for tid in written:
            print(f"             • teams/{tid}")
    if skipped:
        print(f"  Skipped  : {len(skipped)}  (no team_id in API response)")
    print(f"  {'=' * 55}")
    print()
    print("  ✓  Done. Check the Firestore console to verify the documents.")
    print(f"     https://console.cloud.google.com/firestore/databases/{config.FIRESTORE_DATABASE}/data/teams")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print()
    print("  01_teams.py  —  Fetch teams and write to Firestore")
    print()

    teams          = fetch_teams()
    preview_teams(teams)
    confirmed      = confirm_write()

    if not confirmed:
        print()
        print("  Cancelled — nothing was written.")
        print()
        return

    written, skipped = write_teams(teams)
    print_summary(written, skipped)


if __name__ == "__main__":
    main()
