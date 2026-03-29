"""
config.py
Shared configuration used by all poplars-local scripts.

HOW TO SET YOUR VALUES:
  Option A (recommended) — set environment variables in your terminal:
    set PLAY_CRICKET_SITE_ID=12345
    set PLAY_CRICKET_API_KEY=your_key_here
    set GCP_PROJECT_ID=your_project_id

  Option B — fill in the FALLBACK values below directly (fine for local dev,
             but do not commit your API key to version control).
"""

import os

# ---------------------------------------------------------------------------
# GCP / Firestore
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "poplardataprocessing")
FIRESTORE_DATABASE = "poplarsdb"

# ---------------------------------------------------------------------------
# Play-Cricket API
# ---------------------------------------------------------------------------
PLAY_CRICKET_SITE_ID = os.environ.get("PLAY_CRICKET_SITE_ID", "5127")
PLAY_CRICKET_API_KEY = os.environ.get("PLAY_CRICKET_API_KEY", "88dd7cfae2181fa8e801b28fb7b5e13c")
PLAY_CRICKET_BASE_URL = "https://www.play-cricket.com/api/v2"

# ---------------------------------------------------------------------------
# Validation — called at the top of each script so you get a clear error
# if something is not set before any API or Firestore calls are made.
# ---------------------------------------------------------------------------
def validate():
    errors = []
    if GCP_PROJECT_ID == "YOUR_PROJECT_ID":
        errors.append("GCP_PROJECT_ID is not set")
    if PLAY_CRICKET_SITE_ID == "YOUR_SITE_ID":
        errors.append("PLAY_CRICKET_SITE_ID is not set")
    if PLAY_CRICKET_API_KEY == "YOUR_API_KEY":
        errors.append("PLAY_CRICKET_API_KEY is not set")
    if errors:
        print()
        print("  [ERROR] The following config values are missing:")
        for e in errors:
            print(f"    • {e}")
        print()
        print("  Set them as environment variables or edit the fallback values in config.py")
        print()
        raise SystemExit(1)