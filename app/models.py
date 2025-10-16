from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict

from google.protobuf import timestamp_pb2, struct_pb2
from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Boolean,
    false,
    ARRAY,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlmodel import SQLModel, Field, Relationship, JSON

Base = declarative_base(metadata=SQLModel.metadata)


class GeometryType(str, Enum):
    point = "point"
    line = "line"
    polygon = "polygon"


class SessionRegistryStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class TMOBase(SQLModel):
    name: str = Field(index=True)
    p_id: Optional[int] = Field(default=None, index=True)
    icon: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    virtual: bool = Field(default=False, nullable=False)
    global_uniqueness: bool = Field(default=True, nullable=False)
    lifecycle_process_definition: Optional[str] = Field(default=None)
    severity_id: Optional[int] = Field(default=None)
    geometry_type: GeometryType | None = Field(default=None)
    materialize: Optional[bool] = Field(default=False, nullable=True)
    points_constraint_by_tmo: Optional[List[int]] = Field(
        default=None, nullable=True
    )
    inherit_location: bool = Field(default=False, nullable=False)
    minimize: bool = Field(default=False, nullable=False)
    line_type: Optional[str] = Field(default=None)


class TMO(TMOBase, table=True):
    id: Optional[int] = Field(default=None, nullable=False, primary_key=True)

    label: List[int] = Field(
        sa_column=Column(ARRAY(Integer), nullable=False, server_default="{}"),
        default=[],
    )

    version: int = Field(default=1, nullable=False)

    latitude: Optional[int] = Field(default=None)
    longitude: Optional[int] = Field(default=None)
    status: Optional[int] = Field(default=None)

    created_by: str = Field(nullable=False)
    modified_by: str = Field(nullable=False)
    creation_date: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )
    modification_date: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )

    primary: List[int] = Field(
        sa_column=Column(JSON, nullable=False), default=[]
    )
    points_constraint_by_tmo: List[int] = Field(
        sa_column=Column(JSON, nullable=True), default=None
    )

    p_id: Optional[int] = Field(
        sa_column=Column(
            Integer, ForeignKey("tmo.id", ondelete="SET NULL"), default=None
        )
    )
    minimize: bool = Field(
        sa_column=Column(
            Boolean, server_default=false(), default=False, nullable=False
        )
    )
    children: List["TMO"] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "TMO.id==TMO.p_id"}
    )

    mos: List["MO"] = Relationship(
        back_populates="tmo", sa_relationship_kwargs={"cascade": "all, delete"}
    )
    tprms: List["TPRM"] = Relationship(
        back_populates="tmo",
        sa_relationship_kwargs={
            "cascade": "all, delete",
            "order_by": "TPRM.id",
        },
    )

    __table_args__ = (UniqueConstraint("name", name="_tmo_name"),)

    # Needed for Column(JSON)
    class Config:
        arbitrary_types_allowed = True

    def to_proto(self):
        res = dict()
        simple_attrs = [
            "id",
            "p_id",
            "virtual",
            "global_uniqueness",
            "latitude",
            "longitude",
            "primary",
            "label",
            "lifecycle_process_definition",
            "severity_id",
            "materialize",
            "status",
            "points_constraint_by_tmo",
            "minimize",
            "version",
        ]
        string_attrs = [
            "name",
            "icon",
            "description",
            "created_by",
            "modified_by",
            "geometry_type",
        ]
        timestamp_attrs = ["creation_date", "modification_date"]

        for i in simple_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = atr_val
        for i in string_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = str(atr_val)

        for i in timestamp_attrs:
            data = getattr(self, i)
            if data:
                proto_timestamp = timestamp_pb2.Timestamp()
                proto_timestamp.FromDatetime(data)
                res[i] = proto_timestamp

        return res


class MOBase(SQLModel):
    p_id: Optional[int] = Field(default=None)
    point_a_id: Optional[int] = Field(default=None)
    point_b_id: Optional[int] = Field(default=None)
    description: Optional[str] = Field(
        default=None, sa_column=Field(sa_type=String(length=1000))
    )
    tmo_id: int
    pov: Optional[Dict] = Field(default=None)
    geometry: Optional[Dict] = Field(default=None)
    model: Optional[str] = Field(default=None)


class MO(MOBase, table=True):
    id: Optional[int] = Field(default=None, nullable=False, primary_key=True)

    version: int = Field(default=1, nullable=False)

    name: Optional[str] = Field(default=None, index=True)
    label: Optional[str] = Field(default=None, index=True, nullable=True)
    pov: Optional[Dict] = Field(default=None, sa_column=Column(JSON))
    geometry: Optional[Dict] = Field(default=None, sa_column=Column(JSON))
    active: bool = Field(default=True, nullable=False)

    latitude: Optional[float] = Field(default=None)
    longitude: Optional[float] = Field(default=None)
    status: Optional[str] = Field(default=None)
    document_count: int = Field(
        sa_column=Column(Integer, server_default="0", default=0)
    )

    tmo_id: int = Field(
        sa_column=Column(
            Integer, ForeignKey("tmo.id", ondelete="CASCADE"), index=True
        )
    )
    tmo: TMO = Relationship(back_populates="mos")

    p_id: Optional[int] = Field(
        sa_column=Column(
            Integer,
            ForeignKey("mo.id", ondelete="SET NULL"),
            index=True,
            default=None,
        )
    )
    children: List["MO"] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "MO.id==MO.p_id"}
    )

    point_a_id: Optional[int] = Field(
        sa_column=Column(
            Integer,
            ForeignKey("mo.id", ondelete="SET NULL"),
            default=None,
            index=True,
        )
    )
    point_a: Optional["MO"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "MO.id==MO.point_a_id",
            "remote_side": "MO.id",
        }
    )
    point_b_id: Optional[int] = Field(
        sa_column=Column(
            Integer,
            ForeignKey("mo.id", ondelete="SET NULL"),
            default=None,
            index=True,
        )
    )
    point_b: Optional["MO"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "MO.id==MO.point_b_id",
            "remote_side": "MO.id",
        }
    )
    creation_date: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )
    modification_date: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )

    prms: List["PRM"] = Relationship(
        back_populates="mo", sa_relationship_kwargs={"cascade": "all, delete"}
    )

    # Needed for Column(JSON)
    class Config:
        arbitrary_types_allowed = True

    def to_proto(self):
        """Returns object data in files format"""
        res = dict()
        simple_attrs = [
            "id",
            "active",
            "latitude",
            "longitude",
            "tmo_id",
            "p_id",
            "point_a_id",
            "point_b_id",
            "model",
            "document_count",
            "version",
            "status",
            "label",
            "description",
        ]
        dict_attrs = ["pov", "geometry"]
        timestamp_attrs = ["creation_date", "modification_date"]
        for i in simple_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = atr_val

        if self.name and self.name is not None:
            res["name"] = str(self.name)

        for i in dict_attrs:
            value = getattr(self, i)
            if value:
                data = struct_pb2.Struct()
                corecting_dict = {str(k): v for k, v in value.items()}
                data.update(corecting_dict)
                res[i] = data

        for attr_name in timestamp_attrs:
            value = getattr(self, attr_name)
            if value:
                proto_timestamp = timestamp_pb2.Timestamp()
                proto_timestamp.FromDatetime(value)
                res[attr_name] = proto_timestamp

        return res


class TPRMBase(SQLModel):
    name: str = Field(index=True)
    description: Optional[str] = Field(default=None)
    val_type: str = Field(nullable=False, index=True)
    multiple: bool = Field(default=False)
    required: bool = Field(default=False)
    returnable: bool = Field(default=False, nullable=False)
    constraint: Optional[str] = Field(default=None)
    prm_link_filter: Optional[str] = Field(default=None)
    group: Optional[str] = Field(default=None)
    tmo_id: int
    field_value: Optional[str] = None


class TPRM(TPRMBase, table=True):
    id: Optional[int] = Field(
        default=None, nullable=False, primary_key=True, index=True
    )

    version: int = Field(default=1, nullable=False)

    created_by: str = Field(nullable=False)
    modified_by: str = Field(nullable=False)
    creation_date: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )
    modification_date: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )

    backward_link: Optional[int] = Field(
        default=None,
        sa_column=Column(
            Integer,
            ForeignKey("tprm.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
        ),
    )

    tmo_id: int = Field(
        sa_column=Column(
            Integer, ForeignKey("tmo.id", ondelete="CASCADE"), index=True
        )
    )
    tmo: TMO = Relationship(back_populates="tprms")

    prms: List["PRM"] = Relationship(
        back_populates="tprm", sa_relationship_kwargs={"cascade": "all, delete"}
    )

    __table_args__ = (
        UniqueConstraint("tmo_id", "name", name="_tprm_tmo_id_name"),
    )

    def to_proto(self):
        res = dict()
        simple_attrs = [
            "id",
            "multiple",
            "required",
            "returnable",
            "tmo_id",
            "version",
        ]
        string_attrs = [
            "name",
            "description",
            "val_type",
            "constraint",
            "prm_link_filter",
            "group",
            "created_by",
            "modified_by",
            "field_value",
        ]
        timestamp_attrs = ["creation_date", "modification_date"]

        for i in simple_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = atr_val
        for i in string_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = str(atr_val)

        for i in timestamp_attrs:
            data = getattr(self, i)
            if data:
                proto_timestamp = timestamp_pb2.Timestamp()
                proto_timestamp.FromDatetime(data)
                res[i] = proto_timestamp

        return res


class PRMBase(SQLModel):
    value: str = Field(index=True)


class PRM(PRMBase, table=True):
    id: Optional[int] = Field(default=None, nullable=False, primary_key=True)

    version: int = Field(default=1, nullable=False)

    tprm_id: int = Field(
        sa_column=Column(
            Integer, ForeignKey("tprm.id", ondelete="CASCADE"), index=True
        )
    )
    tprm: TPRM = Relationship(back_populates="prms")

    mo_id: int = Field(
        sa_column=Column(
            Integer, ForeignKey("mo.id", ondelete="CASCADE"), index=True
        )
    )
    mo: MO = Relationship(back_populates="prms")

    backward_link: Optional[int] = Field(
        default=None,
        sa_column=Column(
            Integer,
            ForeignKey("prm.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
        ),
    )

    __table_args__ = (
        UniqueConstraint("tprm_id", "mo_id", name="_prm_tprm_id_mo_id"),
    )

    def to_proto(self):
        """Returns object data in files format"""
        res = dict()
        simple_attrs = ["id", "tprm_id", "mo_id", "version"]
        string_attrs = ["value"]
        for i in simple_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = atr_val
        for i in string_attrs:
            atr_val = getattr(self, i)
            if atr_val is not None:
                res[i] = str(atr_val)
        return res


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String)
    model_id = Column(Integer, index=True)
    user = Column(String, default="")
    event_time = Column(DateTime, default=datetime.utcnow, index=True)
    event = Column(JSONB)

    __table_args__ = (
        Index("ix_events_event_type_model_id", "event_type", "model_id"),
    )


class BackgroundTaskBase(SQLModel):
    task_id: str = Field(nullable=False, primary_key=True)


class BackgroundTask(BackgroundTaskBase, table=True):
    task_name: str = Field(nullable=False)
    username: str = Field(nullable=False)
    object_type_id: int = Field(
        sa_column=Column(Integer, ForeignKey(column="tmo.id"), index=True)
    )


class SessionRegistryBase(SQLModel):
    id: Optional[int] = Field(default=None, nullable=False, primary_key=True)


class SessionRegistry(SessionRegistryBase, table=True):
    user_id: str = Field(nullable=False)
    session_id: str = Field(nullable=False)
    activation_datetime: datetime = Field(
        default_factory=datetime.utcnow, nullable=False
    )
    deactivation_datetime: datetime = Field(nullable=True)
    status: SessionRegistryStatus = Field(
        nullable=True, default=SessionRegistryStatus.ACTIVE.value
    )
