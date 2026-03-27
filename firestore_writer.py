"""
firestore_writer.py
All Firestore write and read operations used by the backfill.
Keeps database calls in one place so main.py stays readable.
"""

import logging
from google.cloud import firestore
from transform import transform_match, transform_innings

log = logging.getLogger(__name__)


class FirestoreWriter:
    def __init__(self, db):
        self.db = db

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    def upsert_team(self, team: dict):
        team_id = str(team.get("id", ""))
        if not team_id:
            return
        self.db.collection("teams").document(team_id).set(
            {
                "team_id":    team_id,
                "site_id":    str(team.get("site_id", "")),
                "name":       team.get("name", ""),
                "is_active":  True,
                "created_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,   # merge=True so we don't overwrite is_active if set manually
        )
        log.debug(f"  Upserted team {team_id}")

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    def upsert_player(self, player: dict):
        member_id = str(player.get("member_id", ""))
        if not member_id:
            return
        # Play-Cricket uses 'known_as' for first name in some API versions
        first_name = player.get("known_as") or player.get("first_name", "")
        self.db.collection("players").document(member_id).set(
            {
                "member_id":    member_id,
                "site_id":      str(player.get("site_id", "")),
                "first_name":   first_name,
                "last_name":    player.get("last_name", ""),
                "is_active":    True,
                "last_updated": firestore.SERVER_TIMESTAMP,
            },
            merge=True,   # merge=True so manually added fields (email, DOB) are preserved
        )
        log.debug(f"  Upserted player {member_id}")

    # ------------------------------------------------------------------
    # Matches
    # ------------------------------------------------------------------

    def match_already_complete(self, match_id: str) -> bool:
        """
        Return True if the match document already exists in Firestore
        AND has a non-empty result.  Used to skip re-fetching completed matches.
        """
        doc = self.db.collection("matches").document(match_id).get()
        if not doc.exists:
            return False
        data = doc.to_dict()
        return bool(data.get("result"))

    def write_match(self, detail: dict, our_team_id: str, season_year: int) -> list:
        """
        Write the match document + both innings subcollection documents.
        Returns a list of (match_id, innings_dict) tuples for stats computation.
        """
        match_id = str(detail.get("id", ""))

        # Write top-level match document
        match_doc = transform_match(detail, our_team_id, season_year)
        self.db.collection("matches").document(match_id).set(match_doc)

        # Write innings subcollection and collect for stats
        innings_tuples = []
        for raw_innings in detail.get("innings", []):
            innings_doc  = transform_innings(raw_innings)
            innings_num  = str(innings_doc["innings_number"])
            (
                self.db.collection("matches")
                       .document(match_id)
                       .collection("innings")
                       .document(innings_num)
                       .set(innings_doc)
            )
            innings_tuples.append((match_id, innings_doc))

        log.debug(f"  Written match {match_id} with {len(innings_tuples)} innings")
        return innings_tuples

    # ------------------------------------------------------------------
    # Seasons
    # ------------------------------------------------------------------

    def upsert_season(self, year: int, league_match_ids: list, friendly_match_ids: list):
        self.db.collection("seasons").document(str(year)).set(
            {
                "year":               year,
                "total_matches":      len(league_match_ids) + len(friendly_match_ids),
                "league_match_ids":   league_match_ids,
                "friendly_match_ids": friendly_match_ids,
                "last_updated":       firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        log.info(f"  Season {year} document updated "
                 f"({len(league_match_ids)} league, {len(friendly_match_ids)} friendly)")
