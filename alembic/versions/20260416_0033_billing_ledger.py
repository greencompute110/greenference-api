"""billing ledger, crypto invoices, stripe sessions

Revision ID: 20260416_0033
Revises: 20260416_0032
Create Date: 2026-04-16
"""
from alembic import op
import sqlalchemy as sa

revision = "20260416_0033"
down_revision = "20260416_0032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Replace float balance columns with integer credits
    op.add_column("users", sa.Column("balance_credits", sa.BigInteger(), server_default="0", nullable=False))
    op.drop_column("users", "balance_tao")
    op.drop_column("users", "balance_usd")

    # Ledger entries — double-entry style audit trail
    op.create_table(
        "ledger_entries",
        sa.Column("entry_id", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False, index=True),
        sa.Column("amount_cents", sa.BigInteger(), nullable=False),
        sa.Column("balance_after", sa.BigInteger(), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False, index=True),
        sa.Column("reference_id", sa.String(128), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Crypto invoices
    op.create_table(
        "crypto_invoices",
        sa.Column("invoice_id", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False, index=True),
        sa.Column("currency", sa.String(32), nullable=False),
        sa.Column("amount_crypto", sa.Float(), nullable=False),
        sa.Column("amount_usd", sa.Float(), nullable=False),
        sa.Column("bonus_pct", sa.Float(), nullable=False, server_default="0"),
        sa.Column("total_credits", sa.BigInteger(), nullable=False),
        sa.Column("deposit_address", sa.String(256), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending", index=True),
        sa.Column("tx_hash", sa.String(256), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Stripe checkout sessions
    op.create_table(
        "stripe_sessions",
        sa.Column("session_id", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False, index=True),
        sa.Column("stripe_session_id", sa.String(256), nullable=False, unique=True),
        sa.Column("amount_usd", sa.Float(), nullable=False),
        sa.Column("amount_cents", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending", index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("stripe_sessions")
    op.drop_table("crypto_invoices")
    op.drop_table("ledger_entries")
    op.drop_column("users", "balance_credits")
    op.add_column("users", sa.Column("balance_tao", sa.Float(), server_default="0"))
    op.add_column("users", sa.Column("balance_usd", sa.Float(), server_default="0"))
