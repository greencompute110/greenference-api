"""audit tables — scorecard history + per-epoch audit reports + probe digest columns

Adds the persistence layer for the Chutes-style audit mechanism: append-only
scorecard history, a signed per-epoch report table, and prompt/response
SHA256 digest columns on probe_results so auditors can detect canned/proxied
miner output.

Revision ID: 20260424_0040
Revises: 20260422_0039
Create Date: 2026-04-24
"""
from alembic import op
import sqlalchemy as sa


revision = "20260424_0040"
down_revision = "20260422_0039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # A.5 — probe prompt/response digests
    op.add_column(
        "probe_results",
        sa.Column("prompt_sha256", sa.String(64), nullable=True),
    )
    op.add_column(
        "probe_results",
        sa.Column("response_sha256", sa.String(64), nullable=True),
    )

    # A.1 — append-only scorecard history
    op.create_table(
        "scorecard_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("epoch_id", sa.String(64), nullable=False, index=True),
        sa.Column("snapshot_id", sa.String(64), nullable=True, index=True),
        sa.Column("hotkey", sa.String(128), nullable=False, index=True),
        sa.Column("capacity_weight", sa.Float(), nullable=False),
        sa.Column("reliability_score", sa.Float(), nullable=False),
        sa.Column("performance_score", sa.Float(), nullable=False),
        sa.Column("security_score", sa.Float(), nullable=False),
        sa.Column("fraud_penalty", sa.Float(), nullable=False),
        sa.Column("utilization_score", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("rental_revenue_bonus", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("final_score", sa.Float(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )

    # A.2 — per-epoch audit report (signed, hash anchored on-chain)
    op.create_table(
        "audit_reports",
        sa.Column("epoch_id", sa.String(64), primary_key=True),
        sa.Column("netuid", sa.Integer(), nullable=False, index=True),
        sa.Column("epoch_start_block", sa.Integer(), nullable=False),
        sa.Column("epoch_end_block", sa.Integer(), nullable=False, index=True),
        sa.Column("report_json", sa.JSON(), nullable=False),
        sa.Column("report_sha256", sa.String(64), nullable=False, index=True),
        sa.Column("signature", sa.Text(), nullable=False, server_default=""),
        sa.Column("signer_hotkey", sa.String(128), nullable=False, server_default=""),
        sa.Column("chain_commitment_tx", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )


def downgrade() -> None:
    op.drop_table("audit_reports")
    op.drop_table("scorecard_history")
    op.drop_column("probe_results", "response_sha256")
    op.drop_column("probe_results", "prompt_sha256")
