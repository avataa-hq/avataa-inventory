from datetime import datetime
from typing import List, Optional, Annotated

from pydantic import BaseModel, AfterValidator
from sqlmodel import SQLModel

from routers.object_type_router.schemas_validator import (
    lifecycle_process_definition_validator,
    geometry_type_validator,
)
from routers.parameter_type_router.schemas import TPRMResponse


class TMOResponse(SQLModel):
    id: int
    version: int
    name: str
    label: List[int]
    p_id: Optional[int]
    latitude: Optional[int]
    longitude: Optional[int]
    status: Optional[int]
    icon: Optional[str]
    description: Optional[str]
    child_count: int
    virtual: bool
    global_uniqueness: bool
    primary: List[int]
    created_by: str
    modified_by: str
    creation_date: datetime
    modification_date: datetime
    lifecycle_process_definition: Optional[str]
    geometry_type: Optional[str]
    severity_id: Optional[int]
    materialize: Optional[bool]
    points_constraint_by_tmo: Optional[List[int]]
    inherit_location: bool
    minimize: bool
    line_type: Optional[str]


class TMOResponseWithParameters(TMOResponse):
    tprms: list[TPRMResponse]


class TMOCreate(BaseModel):
    name: str
    p_id: Optional[int] | None = None
    icon: Optional[str] | None = None
    description: Optional[str] = None
    virtual: bool = False
    global_uniqueness: bool = False
    lifecycle_process_definition: Optional[
        Annotated[str, AfterValidator(lifecycle_process_definition_validator)]
    ] = None
    geometry_type: Optional[
        Annotated[str, AfterValidator(geometry_type_validator)]
    ] = None

    materialize: Optional[bool] = True
    points_constraint_by_tmo: Optional[List[int]] = []
    inherit_location: bool = False
    minimize: bool = False
    line_type: str | None = None


class TMOUpdate(SQLModel):
    version: int
    name: Optional[str] | None = None
    label: Optional[List[int]] | None = None
    p_id: Optional[int] | None = None
    latitude: Optional[int] | None = None
    longitude: Optional[int] | None = None
    status: Optional[int] | None = None
    icon: Optional[str] | None = None
    description: Optional[str] | None = None
    virtual: Optional[bool] | None = None
    global_uniqueness: Optional[bool] | None = None
    primary: Optional[List[int]] | None = None
    lifecycle_process_definition: Optional[
        Annotated[str, AfterValidator(lifecycle_process_definition_validator)]
    ] = None
    severity_id: Optional[int] = None
    geometry_type: Optional[
        Annotated[str, AfterValidator(geometry_type_validator)]
    ] = None

    materialize: Optional[bool] | None = None
    points_constraint_by_tmo: Optional[List[int]] | None = None
    inherit_location: bool = False
    minimize: Optional[bool] | None = None
    line_type: str | None = None


class GetObjectTypesRequest(BaseModel):
    object_types_ids: List[int] | None
    with_parameter_types: bool | None


class TMOUpdateRequest(TMOUpdate):
    object_type_id: int


class GetObjectTypeChildRequest(BaseModel):
    parent_id: int


class GetChildrenOfObjectTypeWithDataRequest(BaseModel):
    object_type_id: int
    with_params: bool


class DeleteObjectTypeRequest(BaseModel):
    object_type_id: int
    delete_children: bool


class SearchObjectTypeRequest(BaseModel):
    object_type_name: str


class GetObjectTypeBreadcrumbsRequest(BaseModel):
    object_type_id: int


class GetBreadcrumbsRequest(BaseModel):
    object_type_id: int


class GetAllChildObjectTypeIdsRequest(BaseModel):
    object_type_id: int


class GetAllChildObjectTypeIdsWithDataRequest(BaseModel):
    object_type_id: int
    with_parameters: bool


class SearchObjectTypesByNameRequest(BaseModel):
    object_type_name: str
