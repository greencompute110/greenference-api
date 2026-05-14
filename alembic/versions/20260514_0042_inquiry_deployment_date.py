"""commercial_inquiries: add deployment_date column

Sales asked for a target/potential deployment date on the lead form. Stored
as a free-text 64-char string (not a real DATE column) so users can type
"ASAP", "Q1 2026", or a real ISO date without us having to validate it
server-side.

Revision ID: 20260514_0042
Revises: 20260514_0041
Create Date: 2026-05-14
"""
from alembic import op
import sqlalchemy as sa


revision = "20260514_0042"
down_revision = "20260514_0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "commercial_inquiries",
        sa.Column(
            "deployment_date",
            sa.String(64),
            nullable=False,
            server_default="",
        ),
    )


def downgrade() -> None:
    op.drop_column("commercial_inquiries", "deployment_date")
