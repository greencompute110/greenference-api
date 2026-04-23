"""Crypto price feed — fetches TAO/USD and Alpha/USD prices."""
from __future__ import annotations

import logging
import os
import time

log = logging.getLogger(__name__)

# Cache TTL in seconds
CACHE_TTL = 300  # 5 minutes

# CoinGecko IDs
COINGECKO_IDS = {
    "tao": "bittensor",
    "alpha": "bittensor",  # Alpha uses TAO price as proxy for now
}

_cache: dict[str, tuple[float, float]] = {}  # currency -> (price, timestamp)


def get_price(currency: str) -> float:
    """Get USD price for a crypto currency. Returns cached value if fresh."""
    currency = currency.lower()

    # Stablecoins
    if currency in ("usdt", "usdc"):
        return 1.0

    # Check cache
    if currency in _cache:
        price, ts = _cache[currency]
        if time.time() - ts < CACHE_TTL:
            return price

    # Try env var override first
    env_key = f"BILLING_PRICE_{currency.upper()}"
    env_price = os.environ.get(env_key)
    if env_price:
        try:
            price = float(env_price)
            _cache[currency] = (price, time.time())
            return price
        except ValueError:
            pass

    # Fetch from CoinGecko
    cg_id = COINGECKO_IDS.get(currency)
    if not cg_id:
        log.warning("No price source for currency=%s, using fallback", currency)
        return _fallback_price(currency)

    try:
        import urllib.request
        import json

        url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        price = float(data[cg_id]["usd"])
        _cache[currency] = (price, time.time())
        log.info("Price feed: %s = $%.2f", currency, price)
        return price
    except Exception as exc:
        log.warning("Price feed fetch failed for %s: %s", currency, exc)
        # Return cached stale value if available
        if currency in _cache:
            return _cache[currency][0]
        return _fallback_price(currency)


def _fallback_price(currency: str) -> float:
    """Conservative fallback prices when feed is unavailable."""
    defaults = {"tao": 400.0, "alpha": 400.0}
    return defaults.get(currency, 1.0)
