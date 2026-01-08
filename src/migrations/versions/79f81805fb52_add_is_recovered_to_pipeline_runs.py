"""add is_recovered to pipeline_runs

Revision ID: 79f81805fb52
Revises: 385f0e8dc673
Create Date: 2025-01-23 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '79f81805fb52'
down_revision: Union[str, Sequence[str], None] = '385f0e8dc673'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'pipeline_runs',
        sa.Column('is_recovered', sa.Boolean(), nullable=False, server_default='false')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('pipeline_runs', 'is_recovered')

