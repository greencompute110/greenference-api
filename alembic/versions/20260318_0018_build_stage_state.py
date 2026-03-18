"""add build stage state

Revision ID: 20260318_0018
Revises: 20260318_0017
Create Date: 2026-03-18 06:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_0018"
down_revision = "20260318_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("build_jobs", sa.Column("last_completed_stage", sa.String(length=64), nullable=True))
    op.add_column("build_jobs", sa.Column("stage_state", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))
    op.create_index(op.f("ix_build_jobs_last_completed_stage"), "build_jobs", ["last_completed_stage"], unique=False)
    op.alter_column("build_jobs", "stage_state", server_default=None)


def downgrade() -> None:
    op.drop_index(op.f("ix_build_jobs_last_completed_stage"), table_name="build_jobs")
    op.drop_column("build_jobs", "stage_state")
    op.drop_column("build_jobs", "last_completed_stage")
