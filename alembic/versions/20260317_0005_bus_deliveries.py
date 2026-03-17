"""add durable bus deliveries

Revision ID: 20260317_0005
Revises: 20260317_0004
Create Date: 2026-03-17 15:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260317_0005"
down_revision = "20260317_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bus_deliveries",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("consumer", sa.String(length=128), nullable=False),
        sa.Column("subject", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_bus_deliveries_event_id", "bus_deliveries", ["event_id"])
    op.create_index("ix_bus_deliveries_consumer", "bus_deliveries", ["consumer"])
    op.create_index("ix_bus_deliveries_subject", "bus_deliveries", ["subject"])
    op.create_index("ix_bus_deliveries_status", "bus_deliveries", ["status"])
    op.create_index("ix_bus_deliveries_available_at", "bus_deliveries", ["available_at"])


def downgrade() -> None:
    op.drop_index("ix_bus_deliveries_available_at", table_name="bus_deliveries")
    op.drop_index("ix_bus_deliveries_status", table_name="bus_deliveries")
    op.drop_index("ix_bus_deliveries_subject", table_name="bus_deliveries")
    op.drop_index("ix_bus_deliveries_consumer", table_name="bus_deliveries")
    op.drop_index("ix_bus_deliveries_event_id", table_name="bus_deliveries")
    op.drop_table("bus_deliveries")
