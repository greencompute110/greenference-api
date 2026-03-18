"""switch workload runtime defaults to cpu textgen

Revision ID: 20260318_0021
Revises: 20260318_0020
Create Date: 2026-03-18 00:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260318_0021"
down_revision = "20260318_0020"
branch_labels = None
depends_on = None


OLD_DEFAULT = (
    '{"runtime_kind":"hf-causal-lm","model_identifier":"sshleifer/tiny-gpt2",'
    '"model_revision":null,"tokenizer_identifier":null}'
)
NEW_DEFAULT = (
    '{"runtime_kind":"local-cpu-textgen","model_identifier":"greenference-local-cpu-textgen",'
    '"model_revision":null,"tokenizer_identifier":null}'
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE workloads SET runtime = :new_default "
            "WHERE runtime = :old_default"
        ),
        {"new_default": NEW_DEFAULT, "old_default": OLD_DEFAULT},
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE workloads SET runtime = :old_default "
            "WHERE runtime = :new_default"
        ),
        {"new_default": NEW_DEFAULT, "old_default": OLD_DEFAULT},
    )
