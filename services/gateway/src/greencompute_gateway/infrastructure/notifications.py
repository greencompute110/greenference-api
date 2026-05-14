"""Outbound notifications for sales + ops events.

Two channels, both wired the same way (Slack/Discord/generic webhook). They
exist as separate env vars so the ops team can mute one without losing the
other:

    SALES_WEBHOOK_URL / SALES_WEBHOOK_KIND  — public /contact-sales leads
    OPS_WEBHOOK_URL   / OPS_WEBHOOK_KIND    — fleet / billing / deployment alerts

Webhook delivery is best-effort. If a webhook fails, the underlying event is
already persisted (ledger / deployment row) — we don't retry, we just log
and move on. Sales never loses a lead because Discord is down.

Tunables (all env, all optional):
    OPS_BIG_RENTAL_GPU_THRESHOLD  — fire on rentals >= N GPUs    (default 4)
    OPS_BIG_TOPUP_USD_THRESHOLD   — fire on top-ups >= $N        (default 500)
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

from greencompute_protocol import CommercialInquiryRecord

log = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 5


def _post_webhook(url: str, kind: str, text: str, structured: dict | None = None) -> None:
    """Send a single Slack/Discord/generic-webhook payload. Never raises."""
    if not url:
        return
    kind = (kind or "slack").strip().lower()
    if kind == "discord":
        body = {"content": _markdown_to_discord(text)}
    elif kind == "generic":
        body = {"text": text}
        if structured is not None:
            body["data"] = structured
    else:
        body = {"text": text}
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            if resp.status >= 400:
                log.warning("webhook returned %s", resp.status)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        log.warning("webhook failed: %s", exc)


def _markdown_to_discord(text: str) -> str:
    """Slack renders *bold*; Discord renders **bold**. Cheap rewrite — only
    acts on lines that wrap the whole line in single asterisks (our heading
    convention)."""
    lines = []
    for line in text.split("\n"):
        if line.startswith("*") and line.endswith("*") and not line.startswith("**"):
            lines.append(f"**{line.strip('*')}**")
        else:
            lines.append(line)
    return "\n".join(lines)


# --- Sales / commercial inquiries ----------------------------------------


def _format_inquiry(inquiry: CommercialInquiryRecord) -> str:
    parts = [
        "*New commercial inquiry*",
        f"Email: {inquiry.email}",
    ]
    if inquiry.name:
        parts.append(f"Name: {inquiry.name}")
    if inquiry.company:
        parts.append(f"Company: {inquiry.company}")
    if inquiry.gpu_count is not None:
        parts.append(f"GPUs: {inquiry.gpu_count}")
    if inquiry.duration:
        parts.append(f"Duration: {inquiry.duration}")
    if inquiry.deployment_date:
        parts.append(f"Target date: {inquiry.deployment_date}")
    if inquiry.budget:
        parts.append(f"Budget: {inquiry.budget}")
    if inquiry.use_case:
        snippet = inquiry.use_case[:500]
        if len(inquiry.use_case) > 500:
            snippet += "…"
        parts.append(f"Use case: {snippet}")
    parts.append(f"Inquiry ID: {inquiry.inquiry_id}")
    return "\n".join(parts)


def notify_commercial_inquiry(inquiry: CommercialInquiryRecord) -> None:
    url = os.environ.get("SALES_WEBHOOK_URL", "").strip()
    kind = os.environ.get("SALES_WEBHOOK_KIND", "slack")
    _post_webhook(url, kind, _format_inquiry(inquiry))


# --- Ops alerts -----------------------------------------------------------


def _ops_url_kind() -> tuple[str, str]:
    return (
        os.environ.get("OPS_WEBHOOK_URL", "").strip(),
        os.environ.get("OPS_WEBHOOK_KIND", "slack"),
    )


def _ops_int(env: str, default: int) -> int:
    try:
        return int(os.environ.get(env, str(default)))
    except (TypeError, ValueError):
        return default


def notify_big_rental(
    *,
    deployment_id: str,
    hotkey: str | None,
    gpu_count: int,
    endpoint: str | None,
) -> None:
    """Fired when a rental >= threshold GPUs lands in READY state."""
    threshold = _ops_int("OPS_BIG_RENTAL_GPU_THRESHOLD", 4)
    if gpu_count < threshold:
        return
    url, kind = _ops_url_kind()
    if not url:
        return
    text = (
        f"*Large rental online*\n"
        f"Deployment: {deployment_id[:12]}\n"
        f"GPUs: {gpu_count}\n"
        f"Miner: {hotkey or 'unknown'}"
    )
    if endpoint:
        text += f"\nEndpoint: {endpoint}"
    _post_webhook(url, kind, text)


def notify_deployment_failure(
    *,
    deployment_id: str,
    hotkey: str | None,
    error: str | None,
) -> None:
    url, kind = _ops_url_kind()
    if not url:
        return
    text = (
        f"*Deployment failed*\n"
        f"Deployment: {deployment_id[:12]}\n"
        f"Miner: {hotkey or 'unknown'}\n"
        f"Reason: {(error or 'unknown')[:300]}"
    )
    _post_webhook(url, kind, text)


def notify_big_topup(
    *,
    user_id: str,
    amount_usd: float,
    source: str,
    reference: str,
) -> None:
    """Fired when a Stripe or crypto top-up exceeds threshold."""
    threshold = _ops_int("OPS_BIG_TOPUP_USD_THRESHOLD", 500)
    if amount_usd < threshold:
        return
    url, kind = _ops_url_kind()
    if not url:
        return
    text = (
        f"*High-value top-up*\n"
        f"User: {user_id[:12]}\n"
        f"Amount: ${amount_usd:.2f}\n"
        f"Source: {source}\n"
        f"Reference: {reference[:32]}"
    )
    _post_webhook(url, kind, text)
