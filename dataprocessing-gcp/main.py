"""Cloud Functions entry module for the dataprocessing-gcp package."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def _load_module(filename: str, module_name: str):
    spec = spec_from_file_location(module_name, ROOT / filename)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {filename}")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


players_module = _load_module("02_players.py", "poplars_players")
sync_module = _load_module("04_sync.py", "poplars_sync")

sync_players = players_module.sync_players
sync = sync_module.sync
