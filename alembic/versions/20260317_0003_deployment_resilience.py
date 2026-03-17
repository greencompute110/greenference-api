"""add deployment resilience fields

Revision ID: 20260317_0003
Revises: 20260317_0002
Create Date: 2026-03-17 02:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260317_0003"
down_revision = "20260317_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "deployments",
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "deployments",
        sa.Column("health_check_failures", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("deployments", "health_check_failures")
    op.drop_column("deployments", "retry_count")
