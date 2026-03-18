"""add product foundation entities

Revision ID: 20260318_0022
Revises: 20260318_0021
Create Date: 2026-03-18 02:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260318_0022"
down_revision = "20260318_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("display_name", sa.String(length=128), nullable=True))
    op.add_column("users", sa.Column("bio", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("website", sa.String(length=255), nullable=True))
    op.add_column(
        "users",
        sa.Column("metadata", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.alter_column("users", "metadata", server_default=None)

    op.add_column("workloads", sa.Column("owner_user_id", sa.String(length=64), nullable=True))
    op.create_index(op.f("ix_workloads_owner_user_id"), "workloads", ["owner_user_id"], unique=False)

    op.add_column("deployments", sa.Column("owner_user_id", sa.String(length=64), nullable=True))
    op.create_index(op.f("ix_deployments_owner_user_id"), "deployments", ["owner_user_id"], unique=False)

    op.create_table(
        "user_secrets",
        sa.Column("secret_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("secret_id"),
    )
    op.create_index(op.f("ix_user_secrets_user_id"), "user_secrets", ["user_id"], unique=False)

    op.create_table(
        "workload_shares",
        sa.Column("share_id", sa.String(length=64), nullable=False),
        sa.Column("workload_id", sa.String(length=64), nullable=False),
        sa.Column("owner_user_id", sa.String(length=64), nullable=False),
        sa.Column("shared_with_user_id", sa.String(length=64), nullable=False),
        sa.Column("permission", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("share_id"),
    )
    op.create_index(op.f("ix_workload_shares_workload_id"), "workload_shares", ["workload_id"], unique=False)
    op.create_index(op.f("ix_workload_shares_owner_user_id"), "workload_shares", ["owner_user_id"], unique=False)
    op.create_index(
        op.f("ix_workload_shares_shared_with_user_id"),
        "workload_shares",
        ["shared_with_user_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_workload_shares_shared_with_user_id"), table_name="workload_shares")
    op.drop_index(op.f("ix_workload_shares_owner_user_id"), table_name="workload_shares")
    op.drop_index(op.f("ix_workload_shares_workload_id"), table_name="workload_shares")
    op.drop_table("workload_shares")

    op.drop_index(op.f("ix_user_secrets_user_id"), table_name="user_secrets")
    op.drop_table("user_secrets")

    op.drop_index(op.f("ix_deployments_owner_user_id"), table_name="deployments")
    op.drop_column("deployments", "owner_user_id")

    op.drop_index(op.f("ix_workloads_owner_user_id"), table_name="workloads")
    op.drop_column("workloads", "owner_user_id")

    op.drop_column("users", "metadata")
    op.drop_column("users", "website")
    op.drop_column("users", "bio")
    op.drop_column("users", "display_name")
