from google.cloud import firestore

PROJECT_ID = "poplardataprocessing"
SITE_ID = "5127"
CLUB_NAME = "Poplars CC"
SEASONS = [2025]

def bootstrap():
    db = firestore.Client(project = PROJECT_ID, database = "poplarsdb")
    batch = db.batch()

    # 1. Club Root Document
    club_ref = db.collection("clubs").document(SITE_ID)
    batch.set(
        club_ref,
        {
            "siteId": SITE_ID,
            "clubName": CLUB_NAME,
            "activeSeasons": SEASONS,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

     # 2. Sync state documents
    batch.set(
        db.collection("sync_state").document(f"players_{SITE_ID}"),
        {
            "siteId": SITE_ID,
            "entity": "players",
            "lastRunAt": None,
            "status": "not_started",
        },
        merge=True,
    )

    for season in SEASONS:
        batch.set(
            db.collection("sync_state").document(f"matches_{SITE_ID}_{season}"),
            {
                "siteId": SITE_ID,
                "entity": "matches",
                "season": season,
                "lastRunAt": None,
                "status": "not_started",
            },
            merge=True,
        )

    # 4) Optional _init docs so collections appear in Firestore console immediately
    db.collection("players").document("_init").set(
        {
            "isBootstrap": True,
            "note": "Delete when first real player import runs",
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    db.collection("matches").document("_init").set(
        {
            "isBootstrap": True,
            "note": "Delete when first real match import runs",
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    db.collection("player_season_stats").document("_init").set(
        {
            "isBootstrap": True,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    db.collection("player_career_stats").document("_init").set(
        {
            "isBootstrap": True,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    print("Firestore bootstrap complete.")


if __name__ == "__main__":
    bootstrap()