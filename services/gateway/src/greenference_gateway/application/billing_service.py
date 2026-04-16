from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from greenference_protocol import CryptoInvoice, LedgerEntry, StripeSession
from greenference_gateway.infrastructure.billing_repository import BillingRepository, InsufficientBalanceError

log = logging.getLogger(__name__)

# Bonus rates (as fractions, not percentages)
BONUS_RATES: dict[str, float] = {
    "stripe": 0.00,
    "usdt": 0.05,
    "usdc": 0.05,
    "tao": 0.10,
    "alpha": 0.20,
}

# Deposit addresses per currency (from env vars)
DEPOSIT_ADDRESSES: dict[str, str] = {
    "usdt": os.environ.get("BILLING_DEPOSIT_USDT", ""),
    "usdc": os.environ.get("BILLING_DEPOSIT_USDC", ""),
    "tao": os.environ.get("BILLING_DEPOSIT_TAO", ""),
    "alpha": os.environ.get("BILLING_DEPOSIT_ALPHA", ""),
}


class BillingService:
    def __init__(self, billing_repo: BillingRepository) -> None:
        self.repo = billing_repo

    # --- Balance ---

    def get_balance(self, user_id: str) -> dict:
        credits = self.repo.get_balance(user_id)
        return {
            "balance_credits": credits,
            "balance_usd": round(credits / 100.0, 2),
        }

    def list_ledger(self, user_id: str, limit: int = 50, offset: int = 0) -> list[LedgerEntry]:
        return self.repo.list_ledger(user_id, limit=limit, offset=offset)

    def check_balance(self, user_id: str, required_cents: int) -> bool:
        return self.repo.get_balance(user_id) >= required_cents

    # --- Stripe top-up ---

    def create_stripe_topup(self, user_id: str, amount_usd: float) -> dict:
        """Create a Stripe checkout session. Returns the checkout URL."""
        from greenference_gateway.infrastructure.stripe_client import create_checkout_session

        amount_cents = int(round(amount_usd * 100))
        stripe_session_id, checkout_url = create_checkout_session(
            amount_cents=amount_cents,
            user_id=user_id,
        )
        ss = StripeSession(
            user_id=user_id,
            stripe_session_id=stripe_session_id,
            amount_usd=amount_usd,
            amount_cents=amount_cents,
        )
        self.repo.create_stripe_session(ss)
        return {
            "session_id": ss.session_id,
            "stripe_session_id": stripe_session_id,
            "checkout_url": checkout_url,
            "amount_usd": amount_usd,
            "amount_cents": amount_cents,
        }

    def confirm_stripe_payment(self, stripe_session_id: str) -> dict | None:
        """Idempotent: credit the user for a completed Stripe session."""
        existing = self.repo.get_stripe_session_by_stripe_id(stripe_session_id)
        if existing is None:
            return None
        if existing.status == "paid":
            return {"already_credited": True, "session_id": existing.session_id}
        ss = self.repo.complete_stripe_session(stripe_session_id)
        if ss is None:
            return None
        self.repo.credit_user(
            user_id=ss.user_id,
            amount_cents=ss.amount_cents,
            kind="topup",
            reference_id=ss.session_id,
            description=f"Stripe payment ${ss.amount_usd:.2f}",
        )
        log.info("Stripe payment credited: user=%s amount=%d", ss.user_id, ss.amount_cents)
        return {"credited": True, "session_id": ss.session_id, "amount_cents": ss.amount_cents}

    # --- Crypto top-up ---

    def create_crypto_invoice(self, user_id: str, currency: str, amount_usd: float) -> dict:
        """Create a crypto deposit invoice."""
        currency = currency.lower()
        bonus_pct = BONUS_RATES.get(currency, 0.0)
        base_cents = int(round(amount_usd * 100))
        bonus_cents = int(round(base_cents * bonus_pct))
        total_credits = base_cents + bonus_cents

        # Calculate crypto amount (for stablecoins it's 1:1, for TAO/Alpha use price feed)
        if currency in ("usdt", "usdc"):
            amount_crypto = amount_usd
        else:
            from greenference_gateway.infrastructure.price_feed import get_price
            price = get_price(currency)
            amount_crypto = round(amount_usd / price, 6) if price > 0 else 0.0

        deposit_address = DEPOSIT_ADDRESSES.get(currency, "")
        invoice = CryptoInvoice(
            user_id=user_id,
            currency=currency,
            amount_crypto=amount_crypto,
            amount_usd=amount_usd,
            bonus_pct=bonus_pct,
            total_credits=total_credits,
            deposit_address=deposit_address,
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        self.repo.create_crypto_invoice(invoice)
        return {
            "invoice_id": invoice.invoice_id,
            "currency": currency,
            "amount_crypto": amount_crypto,
            "amount_usd": amount_usd,
            "bonus_pct": bonus_pct,
            "total_credits": total_credits,
            "deposit_address": deposit_address,
            "expires_at": invoice.expires_at.isoformat(),
        }

    def confirm_crypto_deposit(self, invoice_id: str, tx_hash: str) -> dict | None:
        """Admin confirms a crypto deposit. Credits user with bonus."""
        invoice = self.repo.get_crypto_invoice(invoice_id)
        if invoice is None:
            return None
        if invoice.status == "confirmed":
            return {"already_confirmed": True, "invoice_id": invoice_id}

        self.repo.confirm_crypto_invoice(invoice_id, tx_hash)
        self.repo.credit_user(
            user_id=invoice.user_id,
            amount_cents=invoice.total_credits,
            kind="topup",
            reference_id=invoice_id,
            description=f"Crypto deposit {invoice.currency.upper()} ${invoice.amount_usd:.2f} (+{int(invoice.bonus_pct*100)}% bonus)",
        )
        log.info(
            "Crypto deposit confirmed: user=%s invoice=%s credits=%d",
            invoice.user_id,
            invoice_id,
            invoice.total_credits,
        )
        return {
            "confirmed": True,
            "invoice_id": invoice_id,
            "total_credits": invoice.total_credits,
        }

    # --- Usage deduction ---

    def deduct_usage(self, user_id: str, deployment_id: str, amount_cents: int) -> LedgerEntry:
        return self.repo.debit_user(
            user_id=user_id,
            amount_cents=amount_cents,
            kind="usage",
            reference_id=deployment_id,
            description=f"GPU usage for deployment {deployment_id[:8]}",
        )


# Singleton — lazily initialized
_billing_service: BillingService | None = None


def get_billing_service() -> BillingService:
    global _billing_service
    if _billing_service is None:
        repo = BillingRepository()
        _billing_service = BillingService(repo)
    return _billing_service
