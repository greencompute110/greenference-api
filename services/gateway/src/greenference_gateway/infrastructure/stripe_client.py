"""Stripe integration — checkout session creation and webhook verification."""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
STRIPE_SUCCESS_URL = os.environ.get("STRIPE_SUCCESS_URL", "https://greenference.ai/billing?status=success")
STRIPE_CANCEL_URL = os.environ.get("STRIPE_CANCEL_URL", "https://greenference.ai/billing?status=cancelled")


def _get_stripe():
    """Lazy import to avoid hard dependency when Stripe is not configured."""
    try:
        import stripe
        if STRIPE_SECRET_KEY:
            stripe.api_key = STRIPE_SECRET_KEY
        return stripe
    except ImportError:
        raise RuntimeError("stripe package not installed — run: pip install stripe")


def create_checkout_session(amount_cents: int, user_id: str) -> tuple[str, str]:
    """Create a Stripe Checkout Session. Returns (session_id, checkout_url)."""
    stripe = _get_stripe()
    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "unit_amount": amount_cents,
                "product_data": {
                    "name": "Greenference Credits",
                    "description": f"${amount_cents / 100:.2f} USD top-up",
                },
            },
            "quantity": 1,
        }],
        mode="payment",
        success_url=STRIPE_SUCCESS_URL,
        cancel_url=STRIPE_CANCEL_URL,
        metadata={"user_id": user_id},
    )
    return session.id, session.url


def verify_webhook_signature(payload: bytes, sig_header: str) -> dict:
    """Verify and parse a Stripe webhook event."""
    stripe = _get_stripe()
    event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    return event
