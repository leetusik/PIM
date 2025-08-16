"""Enum market

Revision ID: b7cbc9959554
Revises: 980f8bb9caf2
Create Date: 2025-08-16 11:21:11.881902

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7cbc9959554'
down_revision: Union[str, Sequence[str], None] = '980f8bb9caf2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create the enum type first
    market_enum = sa.Enum('KOSPI', 'KOSDAQ', name='market')
    market_enum.create(op.get_bind())
    
    # Update existing data to use enum values (if any)
    op.execute("UPDATE stocks SET market = 'KOSPI' WHERE market IS NULL OR market = ''")
    
    # Alter the column to use the enum type with explicit casting
    op.execute("ALTER TABLE stocks ALTER COLUMN market TYPE market USING market::market")


def downgrade() -> None:
    """Downgrade schema."""
    # Alter the column back to VARCHAR
    op.alter_column('stocks', 'market',
               existing_type=sa.Enum('KOSPI', 'KOSDAQ', name='market'),
               type_=sa.VARCHAR(),
               existing_nullable=True)
    
    # Drop the enum type
    market_enum = sa.Enum('KOSPI', 'KOSDAQ', name='market')
    market_enum.drop(op.get_bind())
