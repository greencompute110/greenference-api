"""deployments.port_mappings column

Added to persist the user-exposed Docker port mappings the node-agent
returns (container_port → host_port). Without this column every SELECT on
`deployments` raises UndefinedColumn because the ORM definition references
it.

Revision ID: 20260421_0034
Revises: 20260416_0033
Create Date: 2026-04-21
"""
from alembic import op
import sqlalchemy as sa


revision = "20260421_0034"
down_revision = "20260416_0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "deployments",
        sa.Column(
            "port_mappings",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )


def downgrade() -> None:
    op.drop_column("deployments", "port_mappings")
