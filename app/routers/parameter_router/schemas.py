from datetime import datetime
from typing import List, Any, Optional, Union, Annotated

from pydantic import BaseModel, BeforeValidator
from sqlmodel import SQLModel

from models import PRMBase


class PRMReadBase(SQLModel):
    id: int
    version: int
    tprm_id: int
    mo_id: int


class PRMReadFloat(PRMReadBase):
    value: float


class PRMReadInt(PRMReadBase):
    value: int


class PRMReadStr(PRMReadBase):
    value: str


class PRMReadBool(PRMReadBase):
    value: bool


class PRMReadMultiple(PRMReadBase):
    value: List[Any]


class PRMCreate(PRMBase):
    value: Any


class PRMRead(PRMReadBase):
    value: Any


class PRMCreateByMO(PRMBase):
    value: Any
    tprm_id: int


class PRMUpdateByMO(PRMBase):
    version: int
    value: Any
    tprm_id: int


class PRMUpdate(PRMBase):
    version: int
    value: Any


class GroupedParam(BaseModel):
    name: str
    description: Optional[str] = None
    val_type: str
    multiple: bool = False
    required: bool = False
    returnable: bool = False
    constraint: Optional[str] = None
    group: Optional[str] = None
    tmo_id: int
    created_by: str
    modified_by: str
    creation_date: datetime
    modification_date: datetime
    tmo_id: int
    tprm_id: int
    mo_id: int
    version: int
    value: Any
    prm_id: Optional[int]


class ResponseGroupedParams(BaseModel):
    name: Union[str, None]
    params: List[GroupedParam]


class NewParameterValue(BaseModel):
    tprm_id: int
    new_value: Any
    version: int | None = None


class MassiveUpdateResponse(BaseModel):
    updated_params: List[PRMRead]


class MassiveParameterDeleteResponse(BaseModel):
    deleted_params: List[PRMRead]


class MassiveCreateResponse(BaseModel):
    created_params: List[PRMRead]


class UpdateParameterByObject(BaseModel):
    object_id: int
    new_values: List[NewParameterValue]


class DeleteParameter(BaseModel):
    object_id: int
    tprm_id: int


class CreateParameterByObject(BaseModel):
    object_id: int
    new_values: List[NewParameterValue]


class PRMInstance(BaseModel):
    mo_id: int
    value: Any
    tprm_id: int
    version: int | None


class CreateObjectParametersResponse(BaseModel):
    data: list
    errors: list


class ParameterData(BaseModel):
    mo_id: int
    prm_id: int
    mo_name: str
    value: Annotated[str, BeforeValidator(lambda _: str(_))]
