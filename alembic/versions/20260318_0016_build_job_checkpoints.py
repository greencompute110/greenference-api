"""add build job checkpoints and recovery fields

Revision ID: 20260318_0016
Revises: 20260318_0015
Create Date: 2026-03-18 05:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_0016"
down_revision = "20260318_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("build_jobs", sa.Column("recovery_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("build_jobs", sa.Column("last_recovered_at", sa.DateTime(timezone=True), nullable=True))
    op.alter_column("build_jobs", "recovery_count", server_default=None)

    op.create_table(
        "build_job_checkpoints",
        sa.Column("checkpoint_id", sa.String(length=64), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("build_id", sa.String(length=64), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("recovered", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("checkpoint_id"),
    )
    op.create_index(op.f("ix_build_job_checkpoints_job_id"), "build_job_checkpoints", ["job_id"], unique=False)
    op.create_index(op.f("ix_build_job_checkpoints_build_id"), "build_job_checkpoints", ["build_id"], unique=False)
    op.create_index(op.f("ix_build_job_checkpoints_attempt"), "build_job_checkpoints", ["attempt"], unique=False)
    op.create_index(op.f("ix_build_job_checkpoints_stage"), "build_job_checkpoints", ["stage"], unique=False)
    op.create_index(op.f("ix_build_job_checkpoints_status"), "build_job_checkpoints", ["status"], unique=False)
    op.create_index(op.f("ix_build_job_checkpoints_created_at"), "build_job_checkpoints", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_build_job_checkpoints_created_at"), table_name="build_job_checkpoints")
    op.drop_index(op.f("ix_build_job_checkpoints_status"), table_name="build_job_checkpoints")
    op.drop_index(op.f("ix_build_job_checkpoints_stage"), table_name="build_job_checkpoints")
    op.drop_index(op.f("ix_build_job_checkpoints_attempt"), table_name="build_job_checkpoints")
    op.drop_index(op.f("ix_build_job_checkpoints_build_id"), table_name="build_job_checkpoints")
    op.drop_index(op.f("ix_build_job_checkpoints_job_id"), table_name="build_job_checkpoints")
    op.drop_table("build_job_checkpoints")
    op.drop_column("build_jobs", "last_recovered_at")
    op.drop_column("build_jobs", "recovery_count")
