from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List, Any

from fastapi import UploadFile
from pydantic import Field, BaseModel
from sqlmodel import SQLModel

from models import MOBase, MO, TPRM
from routers.parameter_router.schemas import PRMCreateByMO


class MOCreate(MOBase):
    pass


class MOUpdate(SQLModel):
    version: int
    p_id: Optional[int] = None
    point_a_id: Optional[int] = None
    point_b_id: Optional[int] = None
    pov: Optional[Dict] = Field(default=None)
    geometry: Optional[Dict] = Field(default=None)
    active: Optional[bool] = Field(default=None)
    description: Optional[str] = Field(default=None)
    model: Optional[str] = Field(default=None)


class MassiveObjectsUpdate(SQLModel):
    object_id: int
    data_for_update: MOUpdate


class MassiveObjectDeleteRequest(SQLModel):
    mo_ids: List[int]
    erase: bool = False
    delete_children: bool = False


class MOCreateWithParams(SQLModel):
    p_id: Optional[int] = Field(default=None)
    point_a_id: Optional[int] = Field(default=None)
    point_b_id: Optional[int] = Field(default=None)
    tmo_id: int
    pov: Optional[Dict] = Field(default=None)
    geometry: Optional[Dict] = Field(default=None)
    params: List[PRMCreateByMO]
    description: Optional[str] = Field(default=None)


class MOReadWithParams(MOBase):
    id: int
    version: int
    name: Optional[str]
    label: Optional[str]
    active: bool
    latitude: Optional[float]
    longitude: Optional[float]
    status: Optional[str]
    params: Optional[List[object]]
    creation_date: Optional[datetime]
    modification_date: Optional[datetime]
    document_count: int
    description: Optional[str]


class MOParamsResponse(BaseModel):
    data: list
    total: int


class MOInheritParent(BaseModel):
    parent_mo: MO | None
    tprm_latitude: int | None
    tprm_longitude: int | None


class ObjectDescendant(BaseModel):
    object_id: int
    parent_id: int | None
    object_name: str
    object_type_id: int
    children: list


class ObjectDescendantsResponse(ObjectDescendant):
    children: list[ObjectDescendant]


@dataclass
class GetObjectRouteRequest:
    object_id: int


@dataclass
class AddModelToObjectRequest:
    object_id: int
    file: UploadFile


class UpdateObjectRequest(MOUpdate):
    object_id: int
    version: int
    p_id: int | None = None
    point_a_id: int | None = None
    point_b_id: int | None = None
    pov: dict | None = None
    geometry: dict | None = None
    active: bool | None = None
    description: str | None = None


@dataclass
class DeleteObjectRequest:
    object_id: int
    erase: bool = (False,)
    delete_child: bool = False


@dataclass
class NewObjectName:
    primary_values: list[str]
    new_object_name: str


@dataclass
class GetChildObjectsWithProcessInstanceIdRequest:
    parent_object_id: int


class GetSiteFiberRequest(BaseModel):
    point_a_id: int


class GetObjectsByNamesRequest(BaseModel):
    tmo_id: int | None = (None,)
    objects_names: List[str] | None = (None,)
    limit: Optional[int] = 50
    offset: Optional[int] = 0
    identifiers_instead_of_values: bool = False


class GetObjectsByNamesResponse(BaseModel):
    data: list = []
    total: int = 0


class GetObjectWithGroupedParametersRequest(BaseModel):
    object_id: int
    only_filled: bool


class GetLinkedObjectsByParametersLinkRequest(BaseModel):
    parameter_type_id: int
    limit: Optional[int] = 50
    offset: Optional[int] = 0


class ParameterDataWithObject(BaseModel):
    prm_id: int
    mo_id: int
    mo_name: str
    prm_value: Any


@dataclass
class LinkedParameterInstances:
    object_instance: MO
    parameter_type_instance: TPRM


class GetAllParentsForObjectRequest(BaseModel):
    object_id: int


class GetAllParentsForObjectMassiveRequest(BaseModel):
    object_ids: list[int]


class GetParentInheritLocationRequest(BaseModel):
    object_id: int


class GetObjectsByParameterRequest(BaseModel):
    parameter_type_id: int
    value: str | None
    limit: int
    offset: int


class GetObjectsByParameterResponse(BaseModel):
    data: list = []
    total: int = 0


class GetObjectsByObjectTypeRequest(BaseModel):
    object_type_ids: List[int]
    show_objects_of_children_object_types: bool
    with_parameters: bool
    active: bool
    limit: Optional[int]
    offset: Optional[int]
    outer_box_longitude_min: Optional[float]
    outer_box_longitude_max: Optional[float]
    outer_box_latitude_min: Optional[float]
    outer_box_latitude_max: Optional[float]
    inner_box_longitude_min: Optional[float]
    inner_box_longitude_max: Optional[float]
    inner_box_latitude_min: Optional[float]
    inner_box_latitude_max: Optional[float]
    identifiers_instead_of_values: bool


class GetObjectsByObjectTypeResponse(BaseModel):
    object_types: list
    objects: list
    results_length: int


class GetObjectWithParametersRequest(BaseModel):
    object_id: int
    with_parameters: bool


@dataclass(frozen=True)
class RebuildGeometryRequest:
    object_type_id: int
    correct: bool
