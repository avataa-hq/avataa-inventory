"""Added materialize attr for TMO models.

Revision ID: f89817487e04
Revises: 79958d7793ed
Create Date: 2023-10-02 16:59:27.548954

"""
from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision = 'f89817487e04'
down_revision = '79958d7793ed'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('tmo', sa.Column('materialize', sa.Boolean(), nullable=True, default=False))


def downgrade():
    op.drop_column('tmo', 'materialize')
