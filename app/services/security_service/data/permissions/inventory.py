from typing import Optional

from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship

from services.security_service.data.permissions.permission_template import (
    PermissionTemplate,
)


class TMOPermission(PermissionTemplate):
    __tablename__ = "tmo_permission"

    root_permission_id: Optional[int] = Column(
        Integer,
        ForeignKey(
            f"{__tablename__}.id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        nullable=True,
        index=True,
    )

    parent_id: int = Column(
        Integer,
        ForeignKey("tmo.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    parent = relationship(
        "TMOPermission",
        backref="child",
        remote_side="TMOPermission.id",
        uselist=False,
    )


class MOPermission(PermissionTemplate):
    __tablename__ = "mo_permission"

    root_permission_id: Optional[int] = Column(
        Integer,
        ForeignKey(
            f"{__tablename__}.id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        nullable=True,
        index=True,
    )

    parent_id: int = Column(
        Integer,
        ForeignKey("mo.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    parent = relationship(
        "MOPermission",
        backref="child",
        remote_side="MOPermission.id",
        uselist=False,
    )


class TPRMPermission(PermissionTemplate):
    __tablename__ = "tprm_permission"

    root_permission_id: Optional[int] = Column(
        Integer,
        ForeignKey(
            f"{__tablename__}.id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        nullable=True,
        index=True,
    )

    parent_id: int = Column(
        Integer,
        ForeignKey("tprm.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    parent = relationship(
        "TPRMPermission",
        backref="child",
        remote_side="TPRMPermission.id",
        uselist=False,
    )
