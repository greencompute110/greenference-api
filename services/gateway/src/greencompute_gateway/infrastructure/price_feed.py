"""Crypto price feed — fetches TAO/USD and Alpha/USD prices.

For TAO we hit CoinGecko's spot price.

For Alpha we compute it from on-chain subnet pool reserves: every Bittensor
subnet has an AMM-style pool of TAO ⇄ Alpha, and the spot price of one Alpha
in TAO equals `SubnetTAO / SubnetAlphaIn` (the two reserve storage entries
on `SubtensorModule`). Then `alpha_usd = (alpha_in_tao) * tao_usd`.

We can't use CoinGecko for Alpha — each subnet's Alpha is its own token,
priced by the pool ratio, not listed on any exchange.
"""
from __future__ import annotations

import logging
import os
import time

log = logging.getLogger(__name__)

# Cache TTL in seconds
CACHE_TTL = 300  # 5 minutes

# CoinGecko IDs — only for tokens with real exchange listings.
COINGECKO_IDS = {
    "tao": "bittensor",
}

_cache: dict[str, tuple[float, float]] = {}  # currency -> (price, timestamp)

# Raw storage keys for SubtensorModule subnet pool reserves. The two 128-bit
# xxhash blocks identify the storage item; the last 4 hex chars are the
# little-endian-encoded u16 netuid. Matches the on-chain query used by
# btcli / explorer tools to read AMM pool state.
#
#   _STORAGE_PREFIX_SUBNET_TAO  → reserve of TAO in the subnet pool
#   _STORAGE_PREFIX_SUBNET_ALPHA → reserve of Alpha in the subnet pool
#
# Alpha price (in TAO) = SubnetTAO / SubnetAlphaIn.
_STORAGE_PREFIX_SUBNET_TAO = (
    "0x658faa385070e074c85bf6b568cf05552ce12f7007574647d692ac7edf8b7a53"
)
_STORAGE_PREFIX_SUBNET_ALPHA = (
    "0x658faa385070e074c85bf6b568cf05557a57dce016211512d1700561066b85a3"
)


def _subtensor_url() -> str:
    return (
        os.environ.get("GREENCOMPUTE_SUBTENSOR_URL")
        or os.environ.get("SUBTENSOR_URL")
        or "wss://entrypoint-finney.opentensor.ai:443/"
    )


def _subnet_netuid() -> int:
    raw = (
        os.environ.get("GREENCOMPUTE_BITTENSOR_NETUID")
        or os.environ.get("BITTENSOR_NETUID")
        or "110"
    )
    try:
        return int(raw)
    except ValueError:
        return 110


def _alpha_storage_key(prefix: str, netuid: int) -> str:
    """Append the netuid (little-endian u16 hex) to the storage-item prefix."""
    netuid_hex = f"{netuid & 0xFF:02x}{(netuid >> 8) & 0xFF:02x}"
    return f"{prefix}{netuid_hex}"


def _fetch_alpha_in_tao(netuid: int, subtensor_url: str) -> float | None:
    """Read the subnet's AMM pool reserves and return alpha-in-TAO ratio.

    Uses substrate-interface's raw `state_getStorage` RPC so we don't depend
    on the chain metadata exposing these reserves as named storage items
    (the names have shifted across Bittensor pallet versions, the storage
    keys themselves are stable).
    """
    try:
        from substrateinterface import SubstrateInterface
    except ImportError:
        log.warning("substrate-interface not installed — cannot fetch alpha price")
        return None

    substrate = None
    try:
        substrate = SubstrateInterface(
            url=subtensor_url,
            ss58_format=42,
            type_registry_preset="substrate-node-template",
        )
        key_tao = _alpha_storage_key(_STORAGE_PREFIX_SUBNET_TAO, netuid)
        key_alpha = _alpha_storage_key(_STORAGE_PREFIX_SUBNET_ALPHA, netuid)
        res_tao = substrate.rpc_request("state_getStorage", [key_tao])
        res_alpha = substrate.rpc_request("state_getStorage", [key_alpha])
        hex_tao = (res_tao or {}).get("result")
        hex_alpha = (res_alpha or {}).get("result")
        if not hex_tao or not hex_alpha:
            log.warning(
                "No alpha pool reserves for netuid=%d (chain returned null)", netuid
            )
            return None
        # Storage values are u64 (or larger) little-endian-encoded ints.
        tao_reserve = int.from_bytes(bytes.fromhex(hex_tao[2:]), "little")
        alpha_reserve = int.from_bytes(bytes.fromhex(hex_alpha[2:]), "little")
        if alpha_reserve <= 0:
            log.warning(
                "Alpha reserve is zero on netuid=%d — pool empty?", netuid
            )
            return None
        return tao_reserve / alpha_reserve
    except Exception as exc:
        log.warning("alpha pool query failed for netuid=%d: %s", netuid, exc)
        return None
    finally:
        if substrate is not None:
            try:
                substrate.close()
            except Exception:
                pass


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

    # Env var override (handy for local dev or manual peg)
    env_key = f"BILLING_PRICE_{currency.upper()}"
    env_price = os.environ.get(env_key)
    if env_price:
        try:
            price = float(env_price)
            _cache[currency] = (price, time.time())
            return price
        except ValueError:
            pass

    # Alpha needs special handling — it's the subnet's own token, priced by
    # the on-chain pool ratio, not a CoinGecko listing.
    if currency == "alpha":
        netuid = _subnet_netuid()
        url = _subtensor_url()
        ratio = _fetch_alpha_in_tao(netuid, url)
        if ratio is None:
            log.warning("Could not derive alpha-in-TAO; using fallback price")
            return _fallback_price(currency)
        tao_usd = get_price("tao")
        price = ratio * tao_usd
        _cache[currency] = (price, time.time())
        log.info(
            "Price feed: alpha = $%.6f  (alpha-in-TAO=%.6f * TAO=$%.2f, netuid=%d)",
            price, ratio, tao_usd, netuid,
        )
        return price

    # Other tokens go through CoinGecko.
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
    # Alpha fallback is intentionally tiny — subnet alphas are typically
    # priced at 0.01–0.1 TAO at launch (so $4–$40 at $400/TAO), and we'd
    # rather under-credit than over-credit if the on-chain query fails.
    defaults = {"tao": 400.0, "alpha": 5.0}
    return defaults.get(currency, 1.0)
