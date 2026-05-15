from __future__ import annotations

import os

from fastapi import HTTPException, status

from greencompute_persistence import CredentialStore, FixedWindowRateLimiter, get_metrics_store
from greencompute_protocol import APIKeyRecord


credential_store = CredentialStore()
rate_limiter = FixedWindowRateLimiter()
metrics = get_metrics_store("greencompute-gateway")


# Synthetic admin record returned when the caller presents the
# GREENCOMPUTE_ADMIN_API_KEY env-var value. Lets ops recover access without
# needing a row in api_keys — break-glass mechanism.
_MASTER_ADMIN_KEY_ID = "env-master"


def _env_master_admin_key() -> str:
    """Read the master admin key on every call so .env changes after restart
    take effect without re-importing."""
    return (os.environ.get("GREENCOMPUTE_ADMIN_API_KEY") or "").strip()


def _master_admin_record(secret: str) -> APIKeyRecord:
    from datetime import UTC, datetime

    return APIKeyRecord(
        key_id=_MASTER_ADMIN_KEY_ID,
        user_id=None,
        name="env master admin",
        admin=True,
        scopes=[],
        secret=secret,
        created_at=datetime.now(UTC),
    )


def extract_api_key_secret(authorization: str | None, x_api_key: str | None) -> str | None:
    if not isinstance(x_api_key, str):
        x_api_key = None
    if not isinstance(authorization, str):
        authorization = None
    if x_api_key:
        return x_api_key
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def require_api_key(
    authorization: str | None,
    x_api_key: str | None,
    *,
    admin_required: bool = False,
):
    secret = extract_api_key_secret(authorization, x_api_key)
    if not secret:
        metrics.increment("auth.failure.missing_api_key")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing api key")
    # Env-var master admin key — break-glass auth that survives DB wipes.
    # Checked BEFORE the DB lookup so it works even when the api_keys table
    # is unreachable (e.g. during incident recovery).
    master = _env_master_admin_key()
    if master and secret == master:
        metrics.increment("auth.success.master_admin")
        return _master_admin_record(secret)
    api_key = credential_store.get_api_key_by_secret(secret)
    if api_key is None:
        metrics.increment("auth.failure.invalid_api_key")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid api key")
    if admin_required and not api_key.admin:
        metrics.increment("auth.failure.admin_required")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin api key required")
    metrics.increment("auth.success")
    return api_key


def enforce_rate_limit(subject: str, key: str, *, limit: int, window_seconds: int) -> None:
    result = rate_limiter.check(subject, key, limit=limit, window_seconds=window_seconds)
    if not result.allowed:
        metrics.increment(f"rate_limit.hit.{subject}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={"message": "rate limit exceeded", "subject": subject, "reset_at": result.reset_at},
        )
