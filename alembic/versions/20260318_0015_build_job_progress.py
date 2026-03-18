"""add build job progress message

Revision ID: 20260318_0015
Revises: 20260318_0014
Create Date: 2026-03-18 03:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_0015"
down_revision = "20260318_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("build_jobs", sa.Column("progress_message", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("build_jobs", "progress_message")
