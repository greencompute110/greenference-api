"""add build execution adapter outputs

Revision ID: 20260317_0011
Revises: 20260317_0010
Create Date: 2026-03-17 00:11:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260317_0011"
down_revision = "20260317_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("builds", sa.Column("registry_manifest_uri", sa.String(length=1024), nullable=True))
    op.add_column("builds", sa.Column("executor_name", sa.String(length=128), nullable=True))
    op.add_column("build_contexts", sa.Column("staged_context_uri", sa.String(length=1024), nullable=True))
    op.add_column("build_contexts", sa.Column("context_manifest_uri", sa.String(length=1024), nullable=True))


def downgrade() -> None:
    op.drop_column("build_contexts", "context_manifest_uri")
    op.drop_column("build_contexts", "staged_context_uri")
    op.drop_column("builds", "executor_name")
    op.drop_column("builds", "registry_manifest_uri")
