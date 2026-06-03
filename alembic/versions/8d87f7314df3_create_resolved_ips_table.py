"""create_resolved_ips_table

Revision ID: 8d87f7314df3
Revises: optimize_indexes_trgm
Create Date: 2026-06-03 09:17:36.470487

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8d87f7314df3'
down_revision: Union[str, Sequence[str], None] = 'optimize_indexes_trgm'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('resolved_ips',
    sa.Column('ip', sa.String(), nullable=False),
    sa.Column('country_code', sa.String(length=16), nullable=True),
    sa.Column('country_name', sa.String(), nullable=True),
    sa.Column('isp', sa.String(), nullable=True),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('ip')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('resolved_ips')
