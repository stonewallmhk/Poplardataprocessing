import api_helpers
import config

api_key = config.get_api_key()
season_year = 2025
url    = f"{config.PLAY_CRICKET_BASE_URL}/matches.json"
params = {
    "api_token": api_key,
    "site_id":   config.PLAY_CRICKET_SITE_ID,
    "season":    season_year,
}

print(f"URL: {url}")
print(f"Params: {params}")

summaries = api_helpers.fetch_match_summaries(api_key, str(season_year))
if not summaries:
    print(f"No matches returned by API for season {season_year}")
api_match_ids = {str(m.get("id", "")) for m in summaries if m.get("id")}
print(f"API returned {len(api_match_ids)} match IDs for {season_year}")