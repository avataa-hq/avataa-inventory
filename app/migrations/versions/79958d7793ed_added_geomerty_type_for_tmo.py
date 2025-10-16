"""Added geomerty type for TMO

Revision ID: 79958d7793ed
Revises: 422ad23caa58
Create Date: 2023-09-22 13:30:55.323389

"""
from enum import Enum
from sqlalchemy.dialects import postgresql

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision = '79958d7793ed'
down_revision = '422ad23caa58'
branch_labels = None
depends_on = None


class GeometryType(str, Enum):
    point = "point"
    line = "line"
    polygon = "polygon"


def upgrade():
    types = postgresql.ENUM(GeometryType, name="geometry_type")
    types.create(op.get_bind(), checkfirst=True)
    op.add_column('tmo', sa.Column('geometry_type',  types))


def downgrade():
    op.drop_column('tmo', 'geometry_type')