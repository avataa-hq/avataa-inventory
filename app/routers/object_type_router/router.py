from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query, Path
from sqlmodel import Session, select

from database import get_session
from functions.db_functions.db_read import get_db_object_type_or_exception
from models import TMO, Event
from routers.object_type_router.exceptions import ObjectTypeCustomException
from routers.object_type_router.processors import (
    GetObjectTypes,
    CreateObjectType,
    UpdateObjectType,
    GetObjectTypeChild,
    GetChildrenOfObjectTypeWithData,
    DeleteObjectType,
    SearchObjectTypes,
    GetObjectTypeBreadcrumbs,
)
from routers.object_type_router.schemas import (
    TMOResponse,
    TMOUpdate,
    TMOCreate,
    TMOResponseWithParameters,
    GetObjectTypesRequest,
    TMOUpdateRequest,
    GetObjectTypeChildRequest,
    GetChildrenOfObjectTypeWithDataRequest,
    DeleteObjectTypeRequest,
    SearchObjectTypeRequest,
    GetObjectTypeBreadcrumbsRequest,
)
from routers.object_type_router.utils import (
    ObjectTypeDBGetter,
)
from routers.parameter_type_router.exceptions import (
    ParameterTypeCustomException,
)

router = APIRouter(tags=["Object types"])


@router.get(
    path="/object_types/", response_model=List[TMOResponseWithParameters]
)
async def read_object_types(
    session: Session = Depends(get_session),
    object_types_ids: List[int] | None = Query(default=None),
    with_tprms: bool | None = Query(default=False),
):
    task = GetObjectTypes(
        session=session,
        request=GetObjectTypesRequest(
            object_types_ids=object_types_ids, with_parameter_types=with_tprms
        ),
    )

    return task.execute()


@router.post("/object_type/", response_model=TMO)
async def create_object_type(
    object_type: TMOCreate, session: Session = Depends(get_session)
):
    try:
        task = CreateObjectType(session=session, request=object_type)
        return task.execute()

    except ObjectTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get(path="/object_type/{id}", response_model=TMO)
async def read_object_type(
    object_type_id: int = Path(..., alias="id"),
    session: Session = Depends(get_session),
):
    try:
        task = ObjectTypeDBGetter(session=session)
        return task._get_object_type_instance_by_id(
            object_type_id=object_type_id
        )

    except ObjectTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.patch(path="/object_type/{id}", response_model=TMO)
async def update_object_type(
    object_type: TMOUpdate,
    object_type_id: int = Path(..., alias="id"),
    session: Session = Depends(get_session),
):
    try:
        task = UpdateObjectType(
            session=session,
            request=TMOUpdateRequest(
                object_type_id=object_type_id,
                **object_type.dict(exclude_unset=True),
            ),
        )

        return task.execute()

    except (ObjectTypeCustomException, ParameterTypeCustomException) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.delete(path="/object_type/{id}")
async def delete_object_type(
    object_type_id: int = Path(..., alias="id"),
    delete_children: bool = Query(False, alias="delete_childs"),
    session: Session = Depends(get_session),
):
    try:
        task = DeleteObjectType(
            session=session,
            request=DeleteObjectTypeRequest(
                object_type_id=object_type_id, delete_children=delete_children
            ),
        )
        return task.execute()

    except ObjectTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get(
    path="/child_object_types/{parent_id}/", response_model=List[TMOResponse]
)
async def read_child_object_types(
    parent_id: int,
    session: Session = Depends(get_session),
):
    """Parent_id must be int. If parent_id eq=0 returns all TMO without parents"""
    try:
        task = GetObjectTypeChild(
            session=session,
            request=GetObjectTypeChildRequest(parent_id=parent_id),
        )
        return task.execute()

    except ObjectTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get(path="/breadcrumbs/{object_type_id}/")
async def read_breadcrumbs(
    object_type_id: int, session: Session = Depends(get_session)
):
    try:
        task = GetObjectTypeBreadcrumbs(
            session=session,
            request=GetObjectTypeBreadcrumbsRequest(
                object_type_id=object_type_id
            ),
        )
        return task.execute()

    except ObjectTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get(path="/search_obj_types/", response_model=List[TMOResponse])
async def search_object_types(
    name: str, session: Session = Depends(get_session)
):
    task = SearchObjectTypes(
        session=session,
        request=SearchObjectTypeRequest(object_type_name=name),
    )
    return task.execute()


@router.get(path="/object_type/{id}/history")
async def get_object_type_history(
    object_type_id: int = Path(..., alias="id"),
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    session: Session = Depends(get_session),
):
    get_db_object_type_or_exception(
        session=session, object_type_id=object_type_id
    )

    tmo_history = session.exec(
        select(Event).where(
            Event.model_id == object_type_id,
            Event.event_time >= date_from if date_from is not None else True,
            Event.event_time <= date_to if date_to is not None else True,
            Event.event_type.in_(["TMOCreate", "TMOUpdate", "TMODelete"]),
        )
    ).all()

    return tmo_history


@router.get(path="/object_type/{id}/all_children_tmo_ids")
async def get_all_children_tmo_ids(
    object_type_id: int = Path(..., alias="id"),
    session: Session = Depends(get_session),
):
    get_db_object_type_or_exception(
        session=session, object_type_id=object_type_id
    )
    response = []

    order = [[object_type_id]]
    for tmo_ids in order:
        stmt = select(TMO.id).where(TMO.p_id.in_(tmo_ids))
        children = session.exec(stmt).all()

        if children:
            order.append(children)
            response.extend(children)

    return response


@router.get(
    path="/object_type/{object_type_id}/all_children_tmos_with_data",
    status_code=200,
)
async def get_all_children_tmos_with_data(
    object_type_id: int,
    with_params: bool = Query(default=False),
    session: Session = Depends(get_session),
):
    try:
        task = GetChildrenOfObjectTypeWithData(
            session=session,
            request=GetChildrenOfObjectTypeWithDataRequest(
                object_type_id=object_type_id, with_params=with_params
            ),
        )
        return task.execute()

    except ObjectTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
