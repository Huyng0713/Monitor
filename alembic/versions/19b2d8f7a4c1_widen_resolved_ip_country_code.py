"""widen_resolved_ip_country_code

Revision ID: 19b2d8f7a4c1
Revises: 8d87f7314df3
Create Date: 2026-06-03 10:42:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "19b2d8f7a4c1"
down_revision: Union[str, Sequence[str], None] = "8d87f7314df3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column(
        "resolved_ips",
        "country_code",
        existing_type=sa.String(length=2),
        type_=sa.String(length=16),
        existing_nullable=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column(
        "resolved_ips",
        "country_code",
        existing_type=sa.String(length=16),
        type_=sa.String(length=2),
        existing_nullable=True,
    )
