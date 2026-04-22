"""model catalog + catalog submissions

Chutes-style shared inference pool. `model_catalog` is the admin-approved
list of models the subnet offers; `model_catalog_submissions` captures
miner-proposed additions that await admin review.

Revision ID: 20260422_0037
Revises: 20260421_0036
Create Date: 2026-04-22
"""
from alembic import op
import sqlalchemy as sa


revision = "20260422_0037"
down_revision = "20260421_0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # `index=True` on the column definition already generates the matching
    # `ix_<table>_<col>` indexes; no separate op.create_index calls needed.
    op.create_table(
        "model_catalog",
        sa.Column("model_id", sa.String(128), primary_key=True),
        sa.Column("display_name", sa.String(255), nullable=False, server_default=""),
        sa.Column("hf_repo", sa.String(255), nullable=False, server_default=""),
        sa.Column("template", sa.String(32), nullable=False, server_default="vllm"),
        sa.Column("min_vram_gb_per_gpu", sa.Integer(), nullable=False, server_default="24"),
        sa.Column("gpu_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_model_len", sa.Integer(), nullable=True),
        sa.Column("visibility", sa.String(16), nullable=False, server_default="public", index=True),
        sa.Column("min_replicas", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_replicas", sa.Integer(), nullable=True),
        sa.Column("admin_notes", sa.Text(), nullable=True),
        sa.Column("created_by_hotkey", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "model_catalog_submissions",
        sa.Column("submission_id", sa.String(64), primary_key=True),
        sa.Column("model_id", sa.String(128), nullable=False, index=True),
        sa.Column("hotkey", sa.String(128), nullable=False, server_default="", index=True),
        sa.Column("signature", sa.Text(), nullable=False, server_default=""),
        sa.Column("display_name", sa.String(255), nullable=False, server_default=""),
        sa.Column("hf_repo", sa.String(255), nullable=False, server_default=""),
        sa.Column("template", sa.String(32), nullable=False, server_default="vllm"),
        sa.Column("min_vram_gb_per_gpu", sa.Integer(), nullable=False, server_default="24"),
        sa.Column("gpu_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_model_len", sa.Integer(), nullable=True),
        sa.Column("rationale", sa.Text(), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending", index=True),
        sa.Column("reviewer_notes", sa.Text(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("model_catalog_submissions")
    op.drop_table("model_catalog")
