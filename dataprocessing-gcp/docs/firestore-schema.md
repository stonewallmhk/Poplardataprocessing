# Firestore Schema

Reference schema for the documents written by the GCP pipeline in `poplarsdb`.

## Collections

Top-level collections used by this project:

- `players`
- `matches`
- `teams`

`teams` is seeded outside the GCP folder, but is part of the overall data model.

## `players/{player_id}`

Player profile document written by `sync_players`.

Example shape:

```json
{
  "member_id": "12345",
  "site_id": "5127",
  "name": "Jane Smith",
  "first_name": "Jane",
  "last_name": "Smith",
  "is_active": true,
  "last_updated": "SERVER_TIMESTAMP"
}
```

Notes:
- document ID is `member_id`
- player writes use `merge=True` to preserve manually-added fields

## `players/{player_id}/season_stats/{year}`

Derived season stats document written by match sync.

Example shape:

```json
{
  "year": 2025,
  "team_id": "52930",
  "matches_played": 12,
  "batting": {
    "innings": 10,
    "not_outs": 2,
    "runs": 412,
    "fours": 47,
    "sixes": 9,
    "highest_score": 88,
    "average": 51.5,
    "fifties": 4,
    "hundreds": 0,
    "ducks": 1
  },
  "bowling": {
    "overs": 38.2,
    "maidens": 6,
    "runs_conceded": 121,
    "wickets": 14,
    "average": 8.64,
    "economy": 3.16,
    "best_bowling": "4/21",
    "five_wickets": 0
  },
  "fielding": {
    "catches": 5,
    "run_outs": 1,
    "stumpings": 0
  },
  "last_updated": "SERVER_TIMESTAMP"
}
```

Batting fields:
- `innings`
- `not_outs`
- `runs`
- `fours`
- `sixes`
- `highest_score`
- `average`
- `fifties`
- `hundreds`
- `ducks`

Bowling fields:
- `overs`
- `maidens`
- `runs_conceded`
- `wickets`
- `average`
- `economy`
- `best_bowling`
- `five_wickets`

Fielding fields:
- `catches`
- `run_outs`
- `stumpings`

## `players/{player_id}/career_stats/summary`

Derived career totals calculated by summing all season stats for that player.

Example shape:

```json
{
  "total_matches": 74,
  "seasons_played": [2019, 2020, 2021, 2022, 2023, 2024, 2025],
  "batting": {
    "innings": 62,
    "not_outs": 9,
    "runs": 2140,
    "fours": 243,
    "sixes": 31,
    "highest_score": 118,
    "average": 40.38,
    "fifties": 14,
    "hundreds": 2,
    "ducks": 4
  },
  "bowling": {
    "overs": 214.4,
    "maidens": 32,
    "runs_conceded": 711,
    "wickets": 63,
    "average": 11.29,
    "economy": 3.31,
    "best_bowling": "6/18",
    "five_wickets": 3
  },
  "fielding": {
    "catches": 18,
    "run_outs": 4,
    "stumpings": 0
  },
  "last_updated": "SERVER_TIMESTAMP"
}
```

Notes:
- document ID is always `summary`
- rebuilt from `season_stats`, not incrementally accumulated across years

## `matches/{match_id}`

Top-level match document written by match sync.

Example shape:

```json
{
  "match_id": "9876543",
  "site_id": "5127",
  "season_year": 2025,
  "status": "Complete",
  "match_type": "league",
  "competition_name": "Essex League Division 2",
  "competition_id": "123",
  "competition_type": "League",
  "league_name": "Essex League",
  "league_id": "999",
  "division": "Essex League Division 2",
  "date": "2025-06-21T00:00:00",
  "match_time": "13:00",
  "venue": "Poplars Ground",
  "ground_id": "456",
  "home_team": {
    "id": "52930",
    "name": "1st XI",
    "club_name": "Poplars CC",
    "club_id": "111"
  },
  "away_team": {
    "id": "88888",
    "name": "1st XI",
    "club_name": "Example CC",
    "club_id": "222"
  },
  "match_officials": {
    "umpire_home": "A Umpire",
    "umpire_home_id": "1",
    "umpire_away": "B Umpire",
    "umpire_away_id": "2",
    "referee": "",
    "referee_id": "",
    "scorer_home": "A Scorer",
    "scorer_home_id": "3",
    "scorer_away": "B Scorer",
    "scorer_away_id": "4"
  },
  "toss_won_by_team_id": "52930",
  "toss": "Poplars won the toss and elected to bat",
  "batted_first": "52930",
  "result": "Won",
  "result_description": "Poplars CC won by 45 runs",
  "result_applied_to": "52930",
  "match_notes": "",
  "last_updated": "SERVER_TIMESTAMP"
}
```

## `matches/{match_id}/innings/{n}`

Per-innings subcollection document written by match sync.

Document IDs:
- `1`
- `2`
- array index order from the API response, not the API `innings_number` field

Example shape:

```json
{
  "innings_number": 1,
  "team_id": "52930",
  "team_name": "Poplars CC 1st XI",
  "total_runs": 227,
  "wickets": 8,
  "overs": 45.0,
  "declared": false,
  "extras": {
    "wides": 7,
    "no_balls": 1,
    "byes": 3,
    "leg_byes": 2,
    "total": 13
  },
  "batting_performances": [
    {
      "player_id": "12345",
      "player_name": "Jane Smith",
      "batting_position": 1,
      "runs": 64,
      "balls_faced": 79,
      "fours": 8,
      "sixes": 1,
      "strike_rate": 81.01,
      "how_out": "ct",
      "dismissal_type": "caught",
      "fielder_name": "Fielder",
      "fielder_id": "54321",
      "bowler_name": "Bowler",
      "bowler_id": "67890"
    }
  ],
  "bowling_performances": [
    {
      "player_id": "67890",
      "player_name": "Bowler",
      "overs": 9.0,
      "maidens": 1,
      "runs_conceded": 31,
      "wickets": 2,
      "wides": 3,
      "no_balls": 0,
      "economy": 3.44
    }
  ],
  "fall_of_wickets": [
    {
      "wicket_number": 1,
      "runs": 42,
      "batsman_out_name": "Jane Smith",
      "batsman_out_id": "12345",
      "batsman_in_name": "Alex Brown",
      "batsman_in_id": "99999"
    }
  ]
}
```

## `teams/{team_id}`

Seeded manually outside the GCP deployment folder.

Typical shape:

```json
{
  "team_id": "52930",
  "site_id": "5127",
  "name": "1st XI",
  "is_active": true,
  "created_at": "SERVER_TIMESTAMP"
}
```

## Data ownership summary

Written by `sync_players`:
- `players/{player_id}`

Written by `sync`:
- `matches/{match_id}`
- `matches/{match_id}/innings/{n}`
- `players/{player_id}/season_stats/{year}`
- `players/{player_id}/career_stats/summary`

Written manually/local scripts:
- `teams/{team_id}`
- initial or manual player enrichment fields not supplied by Play-Cricket
