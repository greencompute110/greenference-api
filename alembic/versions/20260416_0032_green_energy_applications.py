"""add green-energy applications and attachments tables

Revision ID: 0032
Revises: 0031
Create Date: 2026-04-16
"""

from alembic import op
import sqlalchemy as sa

revision = "20260416_0032"
down_revision = "20260407_0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "green_energy_applications",
        sa.Column("application_id", sa.String(64), primary_key=True),
        sa.Column("hotkey", sa.String(128), nullable=False, index=True),
        sa.Column("signature", sa.Text(), server_default=""),
        sa.Column("organization", sa.String(255), server_default=""),
        sa.Column("energy_source", sa.String(128), server_default=""),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(32), server_default="pending", index=True),
        sa.Column("reviewer_notes", sa.Text(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "green_energy_attachments",
        sa.Column("attachment_id", sa.String(64), primary_key=True),
        sa.Column("application_id", sa.String(64), nullable=False, index=True),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("content_type", sa.String(128), server_default="application/octet-stream"),
        sa.Column("size_bytes", sa.Integer(), server_default="0"),
        sa.Column("data_b64", sa.Text(), server_default=""),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("green_energy_attachments")
    op.drop_table("green_energy_applications")
