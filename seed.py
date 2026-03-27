"""
seed.py
One-time interactive script to manually seed the clubs/{site_id} and
team_seasons/{team_id}_{year} documents into Firestore before the backfill runs.

Run locally with:
    pip install google-cloud-firestore
    python seed.py

Requirements:
  - gcloud CLI installed and authenticated  (gcloud auth application-default login)
  - Your GCP project set                    (gcloud config set project YOUR_PROJECT_ID)
"""

from datetime import datetime
from google.cloud import firestore

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = None   # Leave as None — will be read from gcloud config automatically
DATABASE   = "poplarsdb"

# ---------------------------------------------------------------------------
# Small input helpers
# ---------------------------------------------------------------------------

def ask(prompt: str, required: bool = True) -> str:
    while True:
        value = input(f"  {prompt}: ").strip()
        if value:
            return value
        if not required:
            return ""
        print("    [!] This field is required. Please enter a value.")


def ask_int(prompt: str, required: bool = True) -> int | None:
    while True:
        value = input(f"  {prompt}: ").strip()
        if not value and not required:
            return None
        try:
            return int(value)
        except ValueError:
            print("    [!] Please enter a whole number.")


def ask_yn(prompt: str) -> bool:
    while True:
        value = input(f"  {prompt} (y/n): ").strip().lower()
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        print("    [!] Please enter y or n.")


def section(title: str):
    print()
    print(f"{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def subsection(title: str):
    print()
    print(f"  --- {title} ---")


# ---------------------------------------------------------------------------
# Club document builder
# ---------------------------------------------------------------------------

def collect_club_data() -> tuple[str, dict]:
    section("CLUB DETAILS  —  clubs/{site_id}")

    site_id = ask("Play-Cricket Site ID (the numeric ID from your Play-Cricket URL)")
    name    = ask("Official club name  (e.g. Plumtree Cricket Club)")
    year    = ask_int("Year established  (e.g. 1890)")
    email   = ask("General contact email address")

    # Grounds
    grounds = []
    print()
    print("  You can add one or more grounds.")
    while True:
        subsection(f"Ground {len(grounds) + 1}")
        ground_name    = ask("Ground name  (e.g. Radcliffe Road Ground)")
        ground_address = ask("Ground address  (e.g. Radcliffe Road, Plumtree, NG12 5HH)")
        is_primary     = ask_yn("Is this the primary home ground?") if grounds == [] else False
        grounds.append({
            "name":       ground_name,
            "address":    ground_address,
            "is_primary": is_primary,
        })
        if not ask_yn("Add another ground?"):
            break

    # Officials
    subsection("Club Secretary")
    sec_name  = ask("Name")
    sec_email = ask("Email")
    sec_phone = ask("Phone", required=False)

    subsection("Club Treasurer")
    tre_name  = ask("Name")
    tre_email = ask("Email")
    tre_phone = ask("Phone", required=False)

    subsection("Club Chairman")
    cha_name  = ask("Name")
    cha_email = ask("Email")
    cha_phone = ask("Phone", required=False)

    # Optional additional officials
    other_officials = []
    if ask_yn("Add any other officials? (e.g. Welfare Officer, Ground Manager)"):
        while True:
            subsection(f"Additional Official {len(other_officials) + 1}")
            other_officials.append({
                "role":  ask("Role  (e.g. Welfare Officer)"),
                "name":  ask("Name"),
                "email": ask("Email"),
            })
            if not ask_yn("Add another official?"):
                break

    doc = {
        "site_id":          site_id,
        "name":             name,
        "year_established": year,
        "active_grounds":   grounds,
        "contact_email":    email,
        "officials": {
            "secretary": {"name": sec_name, "email": sec_email, "phone": sec_phone},
            "treasurer": {"name": tre_name, "email": tre_email, "phone": tre_phone},
            "chairman":  {"name": cha_name, "email": cha_email, "phone": cha_phone},
            "other":     other_officials,
        },
        "last_updated": firestore.SERVER_TIMESTAMP,
    }

    return site_id, doc


# ---------------------------------------------------------------------------
# Team season document builder
# ---------------------------------------------------------------------------

def collect_team_season_data() -> list[tuple[str, dict]]:
    section("TEAM SEASON DETAILS  —  team_seasons/{team_id}_{year}")

    print()
    print("  This records captain, division and ground for each team per season.")
    print("  Note: team_id and member_id values come from Play-Cricket.")
    print("  You can find team_ids by checking your Play-Cricket fixtures page URL.")

    team_seasons = []
    current_year = datetime.now().year

    while True:
        subsection(f"Team Season {len(team_seasons) + 1}")

        team_id        = ask("Play-Cricket Team ID")
        year           = ask_int(f"Season year  (e.g. {current_year})")
        captain_id     = ask("Captain's Play-Cricket member_id")
        vice_cap_id    = ask("Vice-captain's Play-Cricket member_id", required=False)
        ground         = ask("Designated home ground name for this season")
        league_name    = ask("League name  (e.g. South Nottinghamshire Cricket League)")
        competition_id = ask("Play-Cricket competition_id for the divisional league", required=False)
        division       = ask("Division name  (e.g. Division 2)")

        # End-of-season fields are optional — may not be known yet
        print()
        print("  End-of-season fields — press Enter to skip if the season is in progress.")
        final_position  = ask_int("Final league position  (e.g. 3)", required=False)
        division_result = ""
        if final_position is not None:
            print("  Division result options: promoted / relegated / same")
            while True:
                division_result = input("  Division result: ").strip().lower()
                if division_result in ("promoted", "relegated", "same", ""):
                    break
                print("    [!] Please enter: promoted, relegated, or same")

        doc_id = f"{team_id}_{year}"
        doc = {
            "team_id":            team_id,
            "year":               year,
            "captain_id":         captain_id,
            "vice_captain_id":    vice_cap_id,
            "designated_ground":  ground,
            "competition": {
                "league_name":      league_name,
                "competition_id":   competition_id,
                "division":         division,
                "final_position":   final_position,
                "division_result":  division_result,
            },
            "league_match_ids":   [],   # populated later by the backfill
            "friendly_match_ids": [],   # populated later by the backfill
        }
        team_seasons.append((doc_id, doc))

        if not ask_yn("Add another team season?"):
            break

    return team_seasons


# ---------------------------------------------------------------------------
# Preview and confirm
# ---------------------------------------------------------------------------

def preview_and_confirm(site_id: str, club_doc: dict, team_seasons: list) -> bool:
    section("PREVIEW — Please review before writing to Firestore")

    print()
    print(f"  clubs/{site_id}")
    print(f"    Name             : {club_doc['name']}")
    print(f"    Year established : {club_doc['year_established']}")
    print(f"    Contact email    : {club_doc['contact_email']}")
    print(f"    Grounds          : {len(club_doc['active_grounds'])} ground(s)")
    for g in club_doc["active_grounds"]:
        primary = " (primary)" if g["is_primary"] else ""
        print(f"                       • {g['name']}{primary}")
    print(f"    Secretary        : {club_doc['officials']['secretary']['name']}")
    print(f"    Treasurer        : {club_doc['officials']['treasurer']['name']}")
    print(f"    Chairman         : {club_doc['officials']['chairman']['name']}")
    if club_doc["officials"]["other"]:
        for o in club_doc["officials"]["other"]:
            print(f"    {o['role']:<17}: {o['name']}")

    print()
    for doc_id, ts in team_seasons:
        print(f"  team_seasons/{doc_id}")
        print(f"    Division         : {ts['competition']['division']}")
        print(f"    League           : {ts['competition']['league_name']}")
        print(f"    Captain ID       : {ts['captain_id']}")
        print(f"    Ground           : {ts['designated_ground']}")

    print()
    return ask_yn("Write these documents to Firestore now?")


# ---------------------------------------------------------------------------
# Write to Firestore
# ---------------------------------------------------------------------------

def write_to_firestore(site_id: str, club_doc: dict, team_seasons: list):
    print()
    print(f"  Connecting to Firestore database '{DATABASE}' ...")
    db = firestore.Client(database=DATABASE)

    # Write club document
    db.collection("clubs").document(site_id).set(club_doc)
    print(f"  ✓  Written  clubs/{site_id}")

    # Write team season documents
    for doc_id, ts_doc in team_seasons:
        db.collection("team_seasons").document(doc_id).set(ts_doc)
        print(f"  ✓  Written  team_seasons/{doc_id}")

    print()
    print("  All documents written successfully.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print()
    print("  Poplars Cricket Club — Firestore Seed Script")
    print("  This script writes the manual data to Firestore before the backfill runs.")
    print()

    site_id, club_doc    = collect_club_data()
    team_seasons         = collect_team_season_data()
    confirmed            = preview_and_confirm(site_id, club_doc, team_seasons)

    if confirmed:
        write_to_firestore(site_id, club_doc, team_seasons)
    else:
        print()
        print("  Cancelled — nothing was written.")

    print()


if __name__ == "__main__":
    main()
