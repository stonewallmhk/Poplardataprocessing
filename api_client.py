"""
api_client.py
Wrapper around the four Play-Cricket API endpoints used by the backfill.
All HTTP calls go through a single requests.Session for connection reuse.
"""

import logging
import requests

log = logging.getLogger(__name__)

BASE_URL = "https://play-cricket.com/api/v1"


class PlayCricketAPI:
    def __init__(self, api_key: str, site_id: str):
        self.api_key = api_key
        self.site_id = site_id
        self.session = requests.Session()

    def _get(self, endpoint: str, params: dict = None) -> dict:
        if params is None:
            params = {}
        params["api_token"] = self.api_key
        url = f"{BASE_URL}/{endpoint}"
        log.debug(f"GET {url} params={params}")
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_teams(self) -> list:
        """GET /api/v1/sites/{site_id}/teams"""
        data = self._get(f"sites/{self.site_id}/teams")
        teams = data.get("teams", [])
        log.info(f"  Teams API returned {len(teams)} teams")
        return teams

    def get_players(self) -> list:
        """GET /api/v1/sites/{site_id}/players"""
        data = self._get(f"sites/{self.site_id}/players")
        players = data.get("players", [])
        log.info(f"  Players API returned {len(players)} players")
        return players

    def get_match_summary(self, year: int, team_id: str) -> list:
        """GET /api/v1/sites/{site_id}/matches?season={year}&team_id={team_id}"""
        data = self._get(
            f"sites/{self.site_id}/matches",
            {"season": year, "team_id": team_id},
        )
        matches = data.get("matches", [])
        log.info(f"  Match Summary API: {len(matches)} matches for team {team_id} in {year}")
        return matches

    def get_match_details(self, match_id: str) -> dict:
        """GET /api/v1/matches/{match_id}"""
        data = self._get(f"matches/{match_id}")
        return data.get("match_detail", {})
