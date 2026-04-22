"""miner payout accrual

Per-successful-inference row captured so we can pay miners out of pocket.
Gateway writes a row every time it debits a user for a completed chat
completion (streaming or blocking). Aggregation + on-chain payout is a
later admin job — this round just captures the ledger.

Revision ID: 20260422_0038
Revises: 20260422_0037
Create Date: 2026-04-22
"""
from alembic import op
import sqlalchemy as sa


revision = "20260422_0038"
down_revision = "20260422_0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "miner_payout_accrual",
        sa.Column("accrual_id", sa.String(64), primary_key=True),
        sa.Column("hotkey", sa.String(128), nullable=False, index=True),
        sa.Column("deployment_id", sa.String(64), nullable=False, index=True),
        sa.Column("workload_id", sa.String(64), nullable=False, index=True),
        sa.Column("request_id", sa.String(64), nullable=False, unique=True),
        sa.Column("model", sa.String(128), nullable=False, server_default=""),
        sa.Column("prompt_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completion_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cents_earned", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )


def downgrade() -> None:
    op.drop_table("miner_payout_accrual")
