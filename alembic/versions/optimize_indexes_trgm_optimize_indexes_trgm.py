"""optimize_indexes_trgm

Revision ID: optimize_indexes_trgm
Revises: 
Create Date: 2026-05-28 13:53:57.993504

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'optimize_indexes_trgm'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Drop redundant single time index (already covered by ix_logs_time_status)
    try:
        op.drop_index("ix_logs_time", table_name="logs")
    except Exception as e:
        print(f"Warning: Could not drop ix_logs_time: {e}")

    # 2. Trigram indexes for ip and path (to support LIKE '%x%' queries)
    op.execute("CREATE INDEX ix_logs_ip_trgm ON logs USING GIN (ip gin_trgm_ops)")
    op.execute("CREATE INDEX ix_logs_path_trgm ON logs USING GIN (path gin_trgm_ops)")

    # 3. Single-column indexes for GROUP BY and filters
    op.create_index("ix_logs_ip", "logs", ["ip"])
    op.create_index("ix_logs_path", "logs", ["path"])
    op.create_index("ix_logs_status", "logs", ["status"])


def downgrade() -> None:
    # Drop single-column indexes
    op.drop_index("ix_logs_status", table_name="logs")
    op.drop_index("ix_logs_path", table_name="logs")
    op.drop_index("ix_logs_ip", table_name="logs")

    # Drop trigram indexes
    op.execute("DROP INDEX IF EXISTS ix_logs_path_trgm")
    op.execute("DROP INDEX IF EXISTS ix_logs_ip_trgm")

    # Re-create redundant single time index
    op.create_index("ix_logs_time", "logs", ["time"])
