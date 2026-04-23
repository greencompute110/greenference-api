from __future__ import annotations

import os
from pathlib import Path


DEFAULT_SQLITE_PATH = Path(__file__).resolve().parents[4] / "greencompute-api.db"


def _env(*keys: str) -> str | None:
    """Read first non-empty env var. Migration-aware: for a GREENFERENCE_X key,
    also checks GREENCOMPUTE_X first so either prefix works during rebrand."""
    for key in keys:
        if key.startswith("GREENFERENCE_"):
            v = os.getenv("GREENCOMPUTE_" + key[len("GREENFERENCE_"):])
            if v:
                return v
        v = os.getenv(key)
        if v:
            return v
    return None


def get_database_url() -> str:
    return _env("GREENFERENCE_DATABASE_URL") or f"sqlite+pysqlite:///{DEFAULT_SQLITE_PATH}"


def should_bootstrap_schema() -> bool:
    val = _env("GREENFERENCE_DB_BOOTSTRAP") or ""
    return val.lower() in {"1", "true", "yes"}
