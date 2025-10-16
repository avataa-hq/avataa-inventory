from typing import Optional

from sqlalchemy import String, Boolean, Column, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped

from models import Base


class PermissionTemplate(Base):
    __abstract__ = True

    id: Mapped[int] = Column(Integer, primary_key=True)
    root_permission_id: Mapped[Optional[int]] = Column(Integer, nullable=True)
    permission: Mapped[str] = Column(String, nullable=False)
    permission_name: Mapped[str] = Column(String, nullable=False)
    create: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    read: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    update: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    delete: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    admin: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    parent_id: Mapped[int] = Column(Integer, primary_key=True)

    __table_args__ = (UniqueConstraint("parent_id", "permission"),)

    def update_from_dict(self, item: dict):
        for key, value in item.items():
            if not hasattr(self, key):
                continue
            setattr(self, key, value)

    def to_dict(self, only_actions: bool = False):
        res = self.__dict__
        if "_sa_instance_state" in res:
            res.pop("_sa_instance_state")
            relationships = self.__mapper__.relationships.keys()
            if relationships:
                for relationship in relationships:
                    if relationship in res:
                        res.pop(relationship)
        if only_actions:
            return {
                "create": res["create"],
                "read": res["read"],
                "update": res["update"],
                "delete": res["delete"],
                "admin": res["admin"],
            }
        return res
