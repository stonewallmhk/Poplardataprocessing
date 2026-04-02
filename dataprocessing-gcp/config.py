"""
config.py
Shared configuration for all poplars scripts.

Designed to work in two modes:
  LOCAL  — run from your Windows machine using environment variables
  GCP    — runs as a Cloud Function; API key fetched from Secret Manager

Environment variables required (set these in your terminal for local use):
  GCP_PROJECT_ID          your GCP project ID
  PLAY_CRICKET_SITE_ID    your Play-Cricket site ID
  PLAY_CRICKET_API_KEY    your Play-Cricket API key  (local only — GCP uses Secret Manager)

On GCP, GOOGLE_CLOUD_PROJECT is set automatically by the runtime.
PLAY_CRICKET_API_KEY is fetched from Secret Manager at runtime.
"""

import os
import logging

# ---------------------------------------------------------------------------
# Environment detection
# K_SERVICE is automatically set by Cloud Run / Cloud Functions runtimes.
# If it is present we are running on GCP; otherwise we are running locally.
# ---------------------------------------------------------------------------
IS_GCP = bool(os.environ.get("K_SERVICE"))

# ---------------------------------------------------------------------------
# GCP / Firestore
# ---------------------------------------------------------------------------
# GOOGLE_CLOUD_PROJECT is set automatically on GCP.
# Locally, set GCP_PROJECT_ID as an environment variable.
GCP_PROJECT_ID     = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID")
FIRESTORE_DATABASE = "poplarsdb"

# ---------------------------------------------------------------------------
# Play-Cricket API
# ---------------------------------------------------------------------------
PLAY_CRICKET_SITE_ID  = os.environ.get("PLAY_CRICKET_SITE_ID", "YOUR_SITE_ID")
PLAY_CRICKET_BASE_URL = "https://www.play-cricket.com/api/v2"
OUR_TEAM_ID           = "52930"   # Poplars CC Play-Cricket team_id

# Seconds to wait between Match Detail API calls to respect rate limits
API_CALL_DELAY_SECONDS = 0.5

# ---------------------------------------------------------------------------
# Logging setup
# On GCP, Cloud Logging automatically captures structured log output.
# Locally, logs go to the console in a human-readable format.
# Both use Python's standard logging module — no code changes needed
# when deploying to GCP.
# ---------------------------------------------------------------------------
def setup_logging():
    """
    Configure logging for the current environment.
    Call once at the top of each script or Cloud Function entry point.
    """
    if IS_GCP:
        # On GCP, plain messages — Cloud Logging adds severity and timestamp
        fmt = "%(levelname)s %(name)s: %(message)s"
    else:
        # Locally, include timestamp for readability
        fmt = "%(asctime)s  %(levelname)-8s  %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

# ---------------------------------------------------------------------------
# Secret Manager — API key
# ---------------------------------------------------------------------------
def get_api_key() -> str:
    """
    Returns the Play-Cricket API key.

    On GCP    : fetched from Secret Manager (no key stored in env vars).
    Locally   : read from PLAY_CRICKET_API_KEY environment variable.
    """
    if IS_GCP:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name   = f"projects/{GCP_PROJECT_ID}/secrets/playcricket-api-key/versions/latest"
        resp   = client.access_secret_version(request={"name": name})
        return resp.payload.data.decode("UTF-8").strip()
    else:
        key = os.environ.get("PLAY_CRICKET_API_KEY", "")
        if not key or key == "YOUR_API_KEY":
            raise RuntimeError(
                "PLAY_CRICKET_API_KEY environment variable is not set.\n"
                "Run: set PLAY_CRICKET_API_KEY=your_key_here"
            )
        return key

# ---------------------------------------------------------------------------
# Firestore client factory
# ---------------------------------------------------------------------------
def get_db():
    """Returns a Firestore client connected to poplarsdb."""
    from google.cloud import firestore
    return firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DATABASE)

# ---------------------------------------------------------------------------
# Validation — for local interactive scripts only (01, 02, 03, 04_season_stats)
# 04_sync.py does NOT call this — it uses get_api_key() at runtime instead.
# ---------------------------------------------------------------------------
def validate():
    errors = []
    if GCP_PROJECT_ID == "YOUR_PROJECT_ID":
        errors.append("GCP_PROJECT_ID is not set  (set GOOGLE_CLOUD_PROJECT or GCP_PROJECT_ID)")
    if PLAY_CRICKET_SITE_ID == "YOUR_SITE_ID":
        errors.append("PLAY_CRICKET_SITE_ID is not set")
    if not IS_GCP:
        key = os.environ.get("PLAY_CRICKET_API_KEY", "YOUR_API_KEY")
        if key == "YOUR_API_KEY":
            errors.append("PLAY_CRICKET_API_KEY is not set")
    if errors:
        print()
        print("  [ERROR] The following config values are missing:")
        for e in errors:
            print(f"    • {e}")
        print()
        print("  Set them as environment variables in your terminal:")
        print("    set GCP_PROJECT_ID=your_project_id")
        print("    set PLAY_CRICKET_SITE_ID=your_site_id")
        print("    set PLAY_CRICKET_API_KEY=your_api_key")
        print()
        raise SystemExit(1)
