"""add metadata column to workloads table

Revision ID: 0031
Revises: 0030
Create Date: 2026-04-07
"""

from alembic import op
import sqlalchemy as sa

revision = "20260407_0031"
down_revision = "20260407_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("workloads", sa.Column("metadata", sa.JSON(), server_default="{}"))


def downgrade() -> None:
    op.drop_column("workloads", "metadata")
