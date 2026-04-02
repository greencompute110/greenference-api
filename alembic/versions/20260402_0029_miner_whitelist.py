"""add miner_whitelist table

Revision ID: 20260402_0029
Revises: 20260326_0028
Create Date: 2026-04-02 00:29:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260402_0029"
down_revision = "20260326_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "miner_whitelist",
        sa.Column("hotkey", sa.String(128), primary_key=True),
        sa.Column("label", sa.String(255), nullable=False, server_default=""),
        sa.Column("energy_source", sa.String(128), nullable=False, server_default=""),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("miner_whitelist")
