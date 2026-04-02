# dataprocessing-gcp

GCP deployment source and runbook for the Poplars Play-Cricket data pipeline.

## Overview

This folder contains the deployable Google Cloud version of the pipeline that:

- syncs players from Play-Cricket into Firestore
- syncs new completed matches into Firestore
- writes innings subcollections for each match
- updates player `season_stats`
- recalculates player `career_stats`

The pipeline is designed to run as private HTTP Cloud Functions Gen 2.

## Functions

### `sync_players`

Source files:
- [main.py](./main.py)
- [02_players.py](./02_players.py)
- [config.py](./config.py)

Purpose:
- fetches players from the Play-Cricket Players API
- upserts documents into `players/{member_id}`

Supported query params:
- `include_historic=yes|no`

Example manual invocation:

```powershell
$TOKEN = gcloud auth print-identity-token
(Invoke-WebRequest `
  -Uri "https://us-central1-poplardataprocessing.cloudfunctions.net/poplars-sync-players?include_historic=yes" `
  -Headers @{ Authorization = "Bearer $TOKEN" }).Content
```

### `sync`

Source files:
- [main.py](./main.py)
- [04_sync.py](./04_sync.py)
- [api_helpers.py](./api_helpers.py)
- [stats_engine.py](./stats_engine.py)
- [config.py](./config.py)

Purpose:
- fetches match summaries for a season from Play-Cricket
- compares API match IDs with existing Firestore match IDs
- fetches full detail only for new matches
- writes top-level match documents and innings subcollections
- updates `season_stats`
- recalculates `career_stats`

Supported query params:
- `season_year=YYYY`

Example manual invocation:

```powershell
$TOKEN = gcloud auth print-identity-token
(Invoke-WebRequest `
  -Uri "https://us-central1-poplardataprocessing.cloudfunctions.net/poplars-sync?season_year=2025" `
  -Headers @{ Authorization = "Bearer $TOKEN" }).Content
```

## Architecture

### Data flow

1. Cloud Function receives authenticated HTTP request.
2. [config.py](./config.py) detects GCP runtime and loads configuration.
3. Play-Cricket API key is read from Secret Manager secret `playcricket-api-key`.
4. Data is fetched from the Play-Cricket API.
5. Documents are written to Firestore database `poplarsdb`.
6. Match sync updates derived player statistics.

### File responsibilities

- [main.py](./main.py): Cloud Functions entry module exposing `sync_players` and `sync`
- [config.py](./config.py): runtime detection, logging setup, Firestore client creation, Secret Manager API key lookup
- [02_players.py](./02_players.py): player sync logic
- [04_sync.py](./04_sync.py): match sync orchestration
- [api_helpers.py](./api_helpers.py): Play-Cricket API calls, data transforms, match/innings Firestore writes
- [stats_engine.py](./stats_engine.py): incremental season stats updates and full career stats recalculation
- [requirements.txt](./requirements.txt): Python runtime dependencies

## GCP environment

Current deployment values:

- Project ID: `poplardataprocessing`
- Region: `us-central1`
- Firestore database: `poplarsdb`
- Firestore location: `nam5`
- Secret name: `playcricket-api-key`
- Service account: `poplars-pipeline@poplardataprocessing.iam.gserviceaccount.com`
- Play-Cricket site ID: `5127`

## Required IAM

The Cloud Functions service account needs:

- `roles/secretmanager.secretAccessor`
- `roles/datastore.user`

For manual invocation by a user, that user also needs invoker permission on the function.

## Required services

The project should have these APIs enabled:

- `cloudfunctions.googleapis.com`
- `run.googleapis.com`
- `cloudbuild.googleapis.com`
- `artifactregistry.googleapis.com`
- `secretmanager.googleapis.com`
- `cloudscheduler.googleapis.com`
- `eventarc.googleapis.com`
- `firestore.googleapis.com`
- `logging.googleapis.com`

## Secrets and environment variables

### Secret Manager

Required secret:
- `playcricket-api-key`

The code reads:
- `projects/{GCP_PROJECT_ID}/secrets/playcricket-api-key/versions/latest`

### Environment variables

Required function environment variables:
- `PLAY_CRICKET_SITE_ID=5127`
- `GCP_PROJECT_ID=poplardataprocessing`

Important:
Use separate `--set-env-vars` flags when deploying from PowerShell to avoid malformed values.

Correct example:

```powershell
--set-env-vars PLAY_CRICKET_SITE_ID=5127 `
--set-env-vars GCP_PROJECT_ID=poplardataprocessing
```

## Deployment

Deploy player sync:

```powershell
gcloud functions deploy poplars-sync-players `
  --gen2 `
  --runtime python311 `
  --region us-central1 `
  --source .\dataprocessing-gcp `
  --entry-point sync_players `
  --trigger-http `
  --no-allow-unauthenticated `
  --service-account poplars-pipeline@poplardataprocessing.iam.gserviceaccount.com `
  --set-env-vars PLAY_CRICKET_SITE_ID=5127 `
  --set-env-vars GCP_PROJECT_ID=poplardataprocessing `
  --timeout 300s
```

Deploy match sync:

```powershell
gcloud functions deploy poplars-sync `
  --gen2 `
  --runtime python311 `
  --region us-central1 `
  --source .\dataprocessing-gcp `
  --entry-point sync `
  --trigger-http `
  --no-allow-unauthenticated `
  --service-account poplars-pipeline@poplardataprocessing.iam.gserviceaccount.com `
  --set-env-vars PLAY_CRICKET_SITE_ID=5127 `
  --set-env-vars GCP_PROJECT_ID=poplardataprocessing `
  --timeout 300s
```

## Scheduling

Recommended production schedule:
- run `poplars-sync` weekly during the season
- run `poplars-sync-players` on demand or on a separate schedule if desired

Example scheduler job for weekly match sync:

```powershell
gcloud scheduler jobs create http poplars-weekly-sync `
  --schedule "0 20 * * 0" `
  --uri "https://us-central1-poplardataprocessing.cloudfunctions.net/poplars-sync" `
  --http-method GET `
  --oidc-service-account-email poplars-pipeline@poplardataprocessing.iam.gserviceaccount.com `
  --location us-central1
```

The query string may be omitted to use the current year, or set explicitly with `season_year` if needed.

## Historic backfill

### Players

To reload historic players:

```powershell
$TOKEN = gcloud auth print-identity-token
(Invoke-WebRequest `
  -Uri "https://us-central1-poplardataprocessing.cloudfunctions.net/poplars-sync-players?include_historic=yes" `
  -Headers @{ Authorization = "Bearer $TOKEN" }).Content
```

### Matches

Historic match backfill is done one season at a time:

```powershell
$TOKEN = gcloud auth print-identity-token
(Invoke-WebRequest `
  -Uri "https://us-central1-poplardataprocessing.cloudfunctions.net/poplars-sync?season_year=2022" `
  -Headers @{ Authorization = "Bearer $TOKEN" }).Content
```

Repeat for each season you want, for example `2021`, `2022`, `2023`.

### Idempotency

The match sync uses a diff between API match IDs and existing Firestore match IDs for the requested season. Re-running the same season should not create duplicate matches.

## Stats model

The match sync updates player stats in two layers:

### `season_stats`

Written under:
- `players/{player_id}/season_stats/{year}`

Includes:
- matches played
- batting stats
- bowling stats
- fielding stats

Batting currently includes:
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

### `career_stats`

Written under:
- `players/{player_id}/career_stats/summary`

Computed by summing all `season_stats` documents for that player.

Batting currently includes:
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

See [docs/firestore-schema.md](./docs/firestore-schema.md) for the document structure.

## Operational notes

### Logging

Logging is configured through [config.py](./config.py).

- locally: readable console logs
- on GCP: logs are captured by Cloud Logging

### Expected behavior of `sync`

For each requested season:
- fetch summaries
- determine which match IDs are new
- skip future fixtures with no result
- write new completed matches
- update season stats for affected players
- update career stats for affected players

### Expected behavior of `sync_players`

- fetch players from the API
- split `name` into `first_name` and `last_name`
- upsert player documents with `merge=True`

## Troubleshooting

### `Permission denied on resource project YOUR_PROJECT_ID`

Cause:
- `GCP_PROJECT_ID` was not set correctly in the deployed function

Fix:
- redeploy with separate `--set-env-vars` entries for `PLAY_CRICKET_SITE_ID` and `GCP_PROJECT_ID`

### 500 Internal Server Error when invoking function

Check logs:

```powershell
gcloud functions logs read poplars-sync --region us-central1 --limit 50
gcloud functions logs read poplars-sync-players --region us-central1 --limit 50
```

Also inspect deployed config:

```powershell
gcloud functions describe poplars-sync --region us-central1
gcloud functions describe poplars-sync-players --region us-central1
```

### `--gen2` not recognized

Cause:
- `gcloud` SDK too old

Fix:

```powershell
gcloud components update
```

### Environment variables appear malformed

Symptom:
- `PLAY_CRICKET_SITE_ID` contains the project ID text as part of its value

Fix:
- use one `--set-env-vars` flag per variable in PowerShell

### Secret access errors

Check:
- secret exists with the exact name `playcricket-api-key`
- service account has `roles/secretmanager.secretAccessor`
- `GCP_PROJECT_ID` matches the actual project

### Firestore access errors

Check:
- service account has `roles/datastore.user`
- database name in [config.py](./config.py) is `poplarsdb`

## Change management

When adding or changing a stat, document:

1. where the raw source value comes from
2. which transformer or stats engine consumes it
3. whether it affects only future syncs or requires a historic recompute
4. whether Firestore schema docs need updating

Recent example:
- added batting `fours` and `sixes` to both `season_stats` and `career_stats`

## Recovery and maintenance

### Rotate Play-Cricket API key

Add a new version to the existing secret:

```powershell
Get-Content .\apikey.txt | gcloud secrets versions add playcricket-api-key --data-file=-
```

No code change is required because the runtime reads the `latest` version.

### Re-run a failed season

Invoke the sync again for that year:

```powershell
$TOKEN = gcloud auth print-identity-token
(Invoke-WebRequest `
  -Uri "https://us-central1-poplardataprocessing.cloudfunctions.net/poplars-sync?season_year=2024" `
  -Headers @{ Authorization = "Bearer $TOKEN" }).Content
```

### Rebuild historic derived stats

If a new stat is added, historic `season_stats` and `career_stats` may need a recompute or backfill. A normal future-season sync will not retroactively change already-written historic seasons unless those seasons are reprocessed.
