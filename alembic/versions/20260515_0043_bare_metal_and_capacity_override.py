"""bare_metal_inquiries + gpu_capacity_overrides

bare_metal_inquiries — dedicated lead capture for the bare-metal node CTA on
the rental page. Separate from commercial_inquiries because the form fields
and sales triage workflow are different.

gpu_capacity_overrides — admin-controlled override for the public capacity
surface. When sales contracts capacity ahead of miners going live, lets the
public /capacity and internal /rental pages advertise cluster size rather
than currently-measured counts.

Revision ID: 20260515_0043
Revises: 20260514_0042
Create Date: 2026-05-15
"""
from alembic import op
import sqlalchemy as sa


revision = "20260515_0043"
down_revision = "20260514_0042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bare_metal_inquiries",
        sa.Column("inquiry_id", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(255), server_default=""),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("company", sa.String(255), server_default=""),
        sa.Column("card_type", sa.String(32), server_default=""),
        sa.Column("node_count", sa.Integer(), nullable=True),
        sa.Column("required_vram_gb", sa.Integer(), nullable=True),
        sa.Column("storage_gb_per_node", sa.Integer(), nullable=True),
        sa.Column("work_type", sa.String(64), server_default=""),
        sa.Column("deployment_date", sa.String(64), server_default=""),
        sa.Column("duration", sa.String(128), server_default=""),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("source_ip", sa.String(64), nullable=True),
        sa.Column("user_agent", sa.String(512), nullable=True),
        sa.Column("status", sa.String(32), server_default="new"),
        sa.Column("review_notes", sa.Text(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_bare_metal_inquiries_email", "bare_metal_inquiries", ["email"])
    op.create_index("ix_bare_metal_inquiries_status", "bare_metal_inquiries", ["status"])
    op.create_index("ix_bare_metal_inquiries_submitted_at", "bare_metal_inquiries", ["submitted_at"])

    op.create_table(
        "gpu_capacity_overrides",
        sa.Column("gpu_model", sa.String(64), primary_key=True),
        sa.Column("total_gpus", sa.Integer(), server_default="0"),
        sa.Column("available_gpus", sa.Integer(), server_default="0"),
        sa.Column("note", sa.String(255), server_default=""),
        sa.Column("updated_by", sa.String(255), server_default=""),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("gpu_capacity_overrides")
    op.drop_index("ix_bare_metal_inquiries_submitted_at", table_name="bare_metal_inquiries")
    op.drop_index("ix_bare_metal_inquiries_status", table_name="bare_metal_inquiries")
    op.drop_index("ix_bare_metal_inquiries_email", table_name="bare_metal_inquiries")
    op.drop_table("bare_metal_inquiries")
