"""add invocation routing metadata

Revision ID: 20260318_0019
Revises: 20260318_0018
Create Date: 2026-03-18 07:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_0019"
down_revision = "20260318_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("invocation_records", sa.Column("routed_host", sa.String(length=255), nullable=True))
    op.add_column("invocation_records", sa.Column("resolution_basis", sa.String(length=64), nullable=True))
    op.add_column("invocation_records", sa.Column("routing_reason", sa.String(length=128), nullable=True))
    op.create_index(op.f("ix_invocation_records_routed_host"), "invocation_records", ["routed_host"], unique=False)
    op.create_index(
        op.f("ix_invocation_records_resolution_basis"),
        "invocation_records",
        ["resolution_basis"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_invocation_records_resolution_basis"), table_name="invocation_records")
    op.drop_index(op.f("ix_invocation_records_routed_host"), table_name="invocation_records")
    op.drop_column("invocation_records", "routing_reason")
    op.drop_column("invocation_records", "resolution_basis")
    op.drop_column("invocation_records", "routed_host")
