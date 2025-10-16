from datetime import datetime
from typing import Optional, Any

from pydantic import BaseModel
from sqlmodel import SQLModel

from models import TPRMBase


class TPRMResponse(TPRMBase):
    id: Optional[int]
    version: int
    created_by: str
    modified_by: str
    creation_date: datetime
    modification_date: datetime
    tmo_id: int


class TPRMCreate(TPRMBase):
    field_value: Optional[Any] = None


class TPRMUpdate(SQLModel):
    version: int
    name: Optional[str] = None
    description: Optional[str] = None
    required: Optional[bool] = None
    field_value: Optional[Any] = None
    returnable: Optional[bool] = None
    constraint: Optional[str] = None
    group: Optional[str] = None
    force: Optional[bool] = None
    prm_link_filter: Optional[str] = None


class TPRMUpdateWithValType(TPRMUpdate):
    val_type: Optional[str]


class TPRMUpdateWithTMO(TPRMUpdate):
    tmo_id: int


class TPRMUpdateValtype(SQLModel):
    version: int
    val_type: str
    field_value: str | None = None
    force: Optional[bool]


class TPRMCreateByTMO(SQLModel):
    name: str
    description: Optional[str] = None
    val_type: str
    multiple: Optional[bool] = False
    required: Optional[bool] = False
    returnable: Optional[bool] = False
    group: Optional[str] = None
    constraint: Optional[str] = None
    prm_link_filter: Optional[str] = None
    field_value: Optional[Any] = None


class TPRMReadWithPrimary(TPRMResponse):
    primary: Optional[bool] = False


class TmoTprmsCreationResponse(BaseModel):
    data: list[TPRMResponse]
    errors: list[dict[str, str]]
