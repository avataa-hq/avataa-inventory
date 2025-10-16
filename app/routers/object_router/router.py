from datetime import datetime
from typing import List, Optional, Union

from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Request,
    Path,
    Depends,
    Response,
    UploadFile,
    File,
    Body,
)
from sqlalchemy import (
    func,
    asc,
    desc,
)
from sqlmodel import Session, select
from starlette.datastructures import ImmutableMultiDict, QueryParams
from starlette.responses import StreamingResponse

from common.common_constant import NAME_DELIMITER
from database import get_session
from functions.db_functions.db_read import (
    get_object_with_parameters,
    get_exists_objects,
)
from models import (
    MO,
    PRM,
    Event,
    TMO,
    TPRM,
)
from routers.object_router.exceptions import ObjectCustomException
from routers.object_router.processors import (
    GetObjectRoute,
    AddModelToObject,
    UpdateObject,
    DeleteObject,
    CreateObjectWithParameters,
    GetChildObjectsWithProcessInstanceId,
    MassiveObjectUpdate,
    GetSiteFiber,
    GetObjectsByNames,
    GetObjectWithGroupedParameters,
    GetLinkedObjectsByParametersLink,
    MassiveObjectDelete,
    GetAllParentsForObject,
    GetParentInheritLocation,
    GetObjectsByParameter,
    ReadObjectByObjectTypes,
    GetObjectWithParameters,
    RebuildGeometry,
    UpdateObjectNamesWithNullNames,
    GetAllParentsForObjectMassive,
)
from routers.object_router.schemas import (
    MOUpdate,
    MOCreateWithParams,
    MOParamsResponse,
    MOInheritParent,
    MassiveObjectDeleteRequest,
    MassiveObjectsUpdate,
    ObjectDescendantsResponse,
    GetObjectRouteRequest,
    AddModelToObjectRequest,
    UpdateObjectRequest,
    DeleteObjectRequest,
    GetChildObjectsWithProcessInstanceIdRequest,
    GetSiteFiberRequest,
    GetObjectsByNamesRequest,
    GetObjectWithGroupedParametersRequest,
    GetLinkedObjectsByParametersLinkRequest,
    GetAllParentsForObjectRequest,
    GetParentInheritLocationRequest,
    GetObjectsByParameterRequest,
    GetObjectsByObjectTypeRequest,
    GetObjectWithParametersRequest,
    RebuildGeometryRequest,
    GetAllParentsForObjectMassiveRequest,
)
from routers.object_router.utils import (
    GetAllChildrenForObject,
)
from routers.object_router.utils import (
    read_objects_with_params,
)
from routers.object_type_router.exceptions import ObjectTypeCustomException
from routers.parameter_router.exceptions import ParameterCustomException
from routers.parameter_router.schemas import (
    ResponseGroupedParams,
)
from routers.parameter_type_router.exceptions import (
    ParameterTypeCustomException,
)

router = APIRouter(tags=["Objects"])


@router.get("/objects/")
async def read_objects(
    request: Request,
    response: Response,
    object_type_id: int = None,
    p_id: int = None,
    name: str = None,
    obj_id: Union[List[int], None] = Query(default=None),
    with_parameters: bool = False,
    active: bool = True,
    limit: Optional[int] = Query(default=50, gt=-1),
    offset: Optional[int] = Query(default=0, gt=-1),
    order_by_tprms_id: list[int] | None = Query(None),
    order_by_asc: list[bool] | None = Query(None),
    session: Session = Depends(get_session),
    identifiers_instead_of_values: bool = Query(False, include_in_schema=False),
):
    """
    Able to use query params to obtain filtered results
    """
    res = read_objects_with_params(
        query_params=request.query_params,
        response=response,
        object_type_id=object_type_id,
        p_id=p_id,
        name=name,
        obj_id=obj_id,
        with_parameters=with_parameters,
        active=active,
        limit=limit,
        offset=offset,
        order_by_tprms_id=order_by_tprms_id,
        order_by_asc=order_by_asc,
        session=session,
        search_rule="start_with",
        identifiers_instead_of_values=identifiers_instead_of_values,
    )
    return res


@router.patch("/object/{id}")
async def update_object(
    object_update_request: MOUpdate,
    object_id: int = Path(..., alias="id"),
    session: Session = Depends(get_session),
):
    try:
        task = UpdateObject(
            session=session,
            request=UpdateObjectRequest(
                object_id=object_id,
                **object_update_request.dict(exclude_unset=True),
            ),
        )
        task.check()
        updated_object = task.execute()

        return get_object_with_parameters(
            session=session,
            db_object=updated_object,
            with_parameters=True,
        )

    except (ObjectCustomException, ObjectTypeCustomException) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.patch("/massive_update_object/")
async def massive_update_object(
    objects_for_update: List[MassiveObjectsUpdate],
    session: Session = Depends(get_session),
):
    try:
        task = MassiveObjectUpdate(
            objects_for_update=objects_for_update, session=session
        )
        task.check()
        return task.execute()

    except (ObjectCustomException, ObjectTypeCustomException) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.delete("/object/{id}")
async def delete_object(
    object_id: int = Path(..., alias="id"),
    erase: bool = False,
    delete_child: bool = Query(False, alias="delete_childs"),
    session: Session = Depends(get_session),
):
    try:
        task = DeleteObject(
            session=session,
            request=DeleteObjectRequest(
                object_id=object_id, erase=erase, delete_child=delete_child
            ),
        )
        task.check()
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/massive_objects_delete")
async def massive_objects_delete(
    object_delete_request: MassiveObjectDeleteRequest,
    session: Session = Depends(get_session),
):
    try:
        task = MassiveObjectDelete(
            session=session, request=object_delete_request
        )
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


@router.get("/object/{id}/route")
async def get_object_route(
    object_id: int = Path(..., alias="id"),
    session: Session = Depends(get_session),
):
    try:
        task = GetObjectRoute(
            session=session, request=GetObjectRouteRequest(object_id=object_id)
        )
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/add_object_model/{mo_id}")
async def add_object_model(
    object_id: int = Path(..., alias="mo_id"),
    session: Session = Depends(get_session),
    file: UploadFile | None = File(default=None),
):
    try:
        task = AddModelToObject(
            session=session,
            request=AddModelToObjectRequest(object_id=object_id, file=file),
        )
        return await task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/objects/")
async def read_objects_by_post_method(
    response: Response,
    filter_params: str = Body(""),
    object_type_id: int = Body(None),
    p_id: int = Body(None),
    name: str = Body(None),
    obj_id: Union[List[int], None] = Body(None),
    with_parameters: bool = Body(False),
    active: bool = Body(True),
    limit: Optional[int] = Body(default=50, gt=-1),
    offset: Optional[int] = Body(default=0, gt=-1),
    order_by_tprms_id: list[int] | None = Body(None),
    order_by_asc: list[bool] | None = Body(None),
    session: Session = Depends(get_session),
    identifiers_instead_of_values: bool = Body(False),
):
    """To obtain results for specific objects with filter conditions set filter_params.

    Conditions must be sent in filter_params.\n
    filter_params example:\n
    tprm_id**8**|**contains**=**value**
    where:
    - tprm_id**number** : number = id of tprm
    - **contains** : filter flag
    - **value** : search value\n"""
    if filter_params:
        filter_params = filter_params.split("&")
        filter_params = [item.split("=") for item in filter_params]
        query_params = ImmutableMultiDict(filter_params)
    else:
        query_params = ImmutableMultiDict()
    query_params = QueryParams(query_params)
    res = read_objects_with_params(
        query_params=query_params,
        response=response,
        object_type_id=object_type_id,
        p_id=p_id,
        name=name,
        obj_id=obj_id,
        with_parameters=with_parameters,
        active=active,
        limit=limit,
        offset=offset,
        order_by_tprms_id=order_by_tprms_id,
        order_by_asc=order_by_asc,
        session=session,
        identifiers_instead_of_values=identifiers_instead_of_values,
    )
    return res


@router.get("/objects/names/", status_code=200)
async def read_objects_names(
    obj_ids: List[int] = Query(), session: Session = Depends(get_session)
):
    """Returns object names"""
    obj_ids = set(obj_ids)
    default_res = dict(zip(obj_ids, obj_ids))

    stmt = select(MO.id, MO.name).where(MO.id.in_(obj_ids))
    objects = session.execute(stmt).all()

    for item in objects:
        if item.name is not None:
            default_res[item.id] = item.name

    return default_res


@router.get("/objects_by_object_types/", status_code=200)
async def read_objects_by_object_type(
    response: Response,
    object_type_ids: List[int] = Query(),
    show_objects_of_children_object_types: bool = False,
    with_parameters: bool = False,
    active: bool = True,
    limit: Optional[int] = Query(default=50, gt=-1),
    offset: Optional[int] = Query(default=0, gt=-1),
    outer_box_longitude_min: Optional[float] = Query(default=None),
    outer_box_longitude_max: Optional[float] = Query(default=None),
    outer_box_latitude_min: Optional[float] = Query(default=None),
    outer_box_latitude_max: Optional[float] = Query(default=None),
    inner_box_longitude_min: Optional[float] = Query(default=None),
    inner_box_longitude_max: Optional[float] = Query(default=None),
    inner_box_latitude_min: Optional[float] = Query(default=None),
    inner_box_latitude_max: Optional[float] = Query(default=None),
    session: Session = Depends(get_session),
    identifiers_instead_of_values: bool = Query(False, include_in_schema=False),
):
    try:
        task = ReadObjectByObjectTypes(
            session=session,
            request=GetObjectsByObjectTypeRequest(
                object_type_ids=object_type_ids,
                show_objects_of_children_object_types=show_objects_of_children_object_types,
                with_parameters=with_parameters,
                active=active,
                limit=limit,
                offset=offset,
                outer_box_longitude_min=outer_box_longitude_min,
                outer_box_longitude_max=outer_box_longitude_max,
                outer_box_latitude_min=outer_box_latitude_min,
                outer_box_latitude_max=outer_box_latitude_max,
                inner_box_longitude_min=inner_box_longitude_min,
                inner_box_longitude_max=inner_box_longitude_max,
                inner_box_latitude_min=inner_box_latitude_min,
                inner_box_latitude_max=inner_box_latitude_max,
                identifiers_instead_of_values=identifiers_instead_of_values,
            ),
        )
        task_response = task.execute()
        response.headers["Result-Length"] = str(task_response.results_length)
        return {
            "object_types": task_response.object_types,
            "objects": task_response.objects,
        }

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/object/{id}")
async def read_object_with_parameters(
    object_id: int = Path(..., alias="id"),
    with_parameters: bool = False,
    session: Session = Depends(get_session),
):
    try:
        task = GetObjectWithParameters(
            session=session,
            request=GetObjectWithParametersRequest(
                object_id=object_id, with_parameters=with_parameters
            ),
        )
        return task.execute()

    except (
        ObjectCustomException,
        ObjectTypeCustomException,
        ParameterCustomException,
        ParameterTypeCustomException,
    ) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/object_with_parameters/")
async def create_object_with_parameters(
    session: Session = Depends(get_session),
    request: MOCreateWithParams = Body(..., alias="object"),
):
    try:
        task = CreateObjectWithParameters(session=session, request=request)
        task.check()
        return task.execute()

    except (
        ObjectCustomException,
        ObjectTypeCustomException,
        ParameterCustomException,
        ParameterTypeCustomException,
    ) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get(
    "/object/{id}/grouped_parameters",
    response_model=List[ResponseGroupedParams],
)
async def get_object_with_grouped_parameters(
    id_: int = Path(..., alias="id"),
    only_filled: bool = False,
    session: Session = Depends(get_session),
):
    try:
        task = GetObjectWithGroupedParameters(
            session=session,
            request=GetObjectWithGroupedParametersRequest(
                object_id=id_, only_filled=only_filled
            ),
        )
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/object/{id}/fiber", response_model=MO, status_code=200)
def get_site_fiber(
    point_a_id: int = Path(..., alias="id"),
    session: Session = Depends(get_session),
):
    try:
        task = GetSiteFiber(
            session=session, request=GetSiteFiberRequest(point_a_id=point_a_id)
        )
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/multi_object_search", status_code=200)
def get_objects_by_names(
    tmo_id: int = None,
    objects_names: List[str] = Query(None),
    session: Session = Depends(get_session),
    limit: Optional[int] = Query(default=50, gt=0),
    offset: Optional[int] = Query(default=0, ge=0),
    identifiers_instead_of_values: bool = Query(False, include_in_schema=False),
):
    task = GetObjectsByNames(
        session=session,
        request=GetObjectsByNamesRequest(
            tmo_id=tmo_id,
            objects_names=objects_names,
            limit=limit,
            offset=offset,
            identifiers_instead_of_values=identifiers_instead_of_values,
        ),
    )
    return task.execute()


@router.get("/object/search_by_prm_values/{tprm_id}", status_code=200)
def search_by_prm_values(
    tprm_id: int,
    limit: Optional[int] = Query(default=50, gt=0),
    offset: Optional[int] = Query(default=0, ge=0),
    session: Session = Depends(get_session),
):
    """
    Endpoint need to get PRM values by some TPRM with val type 'prm_link'. The problem is: TPRM with
    this val type store prm id in PRM value, so we need to get values from ids, it values of specific TPRM with
    val type 'prm link'

    :param tprm_id: id of TPRM with val type 'prm link'
    :param limit: limit
    :param offset: offset
    :param session: database session
    :return: list of dicts PRM id,PRM value (list), MO id, MO name
    """
    try:
        task = GetLinkedObjectsByParametersLink(
            session=session,
            request=GetLinkedObjectsByParametersLinkRequest(
                parameter_type_id=tprm_id, limit=limit, offset=offset
            ),
        )
        return task.execute()

    except (
        ObjectCustomException,
        ParameterCustomException,
        ParameterTypeCustomException,
    ) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/object/search_by_values/{tprm_id}", status_code=200)
def get_objects_by_prm(
    tprm_id: int,
    value: str = None,
    limit: Optional[int] = Query(default=50, gt=0),
    offset: Optional[int] = Query(default=0, ge=0),
    session: Session = Depends(get_session),
):
    """
    Endpoint search values by TPRM id. If value in request - we search by specfic value, which can be in PRM value.
    If it's not exists -- we give values by limit of requested TPRM id.
    :param tprm_id: TPRM id with any val type (except 'prm link')
    :param value: optional parameter of value, by which we can proccess search
    :param limit: limit
    :param offset: offset
    :param session: database session
    :return: list of dicts PRM id,PRM value (list), MO id, MO name and total value quantity
    """
    try:
        task = GetObjectsByParameter(
            session=session,
            request=GetObjectsByParameterRequest(
                parameter_type_id=tprm_id,
                value=value,
                limit=limit,
                offset=offset,
            ),
        )
        return task.execute()

    except ParameterTypeCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


@router.get(
    "/object/{mo_id}/inherit_location",
    response_model=MOInheritParent,
)
def get_parent_inherit_location(
    object_id: int = Path(..., alias="mo_id"),
    session: Session = Depends(get_session),
):
    try:
        task = GetParentInheritLocation(
            session=session,
            request=GetParentInheritLocationRequest(object_id=object_id),
        )
        return task.execute()

    except (ObjectCustomException, ObjectTypeCustomException) as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


@router.get("/get_all_parent/{mo_id}")
async def get_all_parent(mo_id: int, session: Session = Depends(get_session)):
    try:
        task = GetAllParentsForObject(
            session=session,
            request=GetAllParentsForObjectRequest(object_id=mo_id),
        )
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


@router.post("/get_all_parent/massive")
async def get_all_parent_massive(
    object_ids: list[int], session: Session = Depends(get_session)
):
    try:
        task = GetAllParentsForObjectMassive(
            session=session,
            request=GetAllParentsForObjectMassiveRequest(object_ids=object_ids),
        )
        return task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


@router.get(
    "/get_all_children/{mo_id}",
)
def get_all_children(mo_id: int, session: Session = Depends(get_session)):
    task = GetAllChildrenForObject(session=session, object_id=mo_id)

    try:
        task.check()
        tree = task.execute()
        return ObjectDescendantsResponse(**tree)

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


@router.get("/get_object_child_with_process_instance_id/{parent_object_id}")
async def get_object_child_with_process_instance_id(
    parent_object_id: int,
    session: Session = Depends(get_session),
):
    try:
        task = GetChildObjectsWithProcessInstanceId(
            session=session,
            request=GetChildObjectsWithProcessInstanceIdRequest(
                parent_object_id=parent_object_id
            ),
        )
        return await task.execute()

    except ObjectCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get(
    path="/get_all_mo_attrs_available_for_hierarchy_levels",
    response_model=List[str],
)
async def get_all_mo_attrs_available_for_hierarchy_levels(
    session: Session = Depends(get_session),
):
    all_mo_attrs = {c.name for c in MO.__table__.c}
    available_attrs = [
        "name",
        "active",
        "status",
        "creation_date",
        "modification_date",
    ]
    return [
        attr_name for attr_name in available_attrs if attr_name in all_mo_attrs
    ]


@router.get("/objects/history")
async def get_objects_history(
    ids: List[int] = Query(),
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: Optional[int] = Query(default=50, gt=-1),
    offset: Optional[int] = Query(default=0, gt=-1),
    ascending: Optional[bool] = True,
    session: Session = Depends(get_session),
):
    objects = get_exists_objects(session=session, mo_ids=ids)
    response = []
    if ascending:
        ascending = asc(Event.event_time)
    else:
        ascending = desc(Event.event_time)
    if objects:
        for obj in objects:
            mo_history = session.exec(
                select(Event)
                .where(
                    Event.model_id == obj.id,
                    Event.event_time >= date_from
                    if date_from is not None
                    else True,
                    Event.event_time <= date_to
                    if date_to is not None
                    else True,
                    Event.event_type.in_(["MOCreate", "MOUpdate", "MODelete"]),
                )
                .order_by(ascending)
            ).all()

            stmt = select(PRM.id).where(PRM.mo_id == obj.id).distinct()
            prm_ids = session.exec(stmt).all()

            prm_history = session.execute(
                select(
                    Event.event_type,
                    Event.user,
                    Event.model_id,
                    Event.event,
                    Event.event_time,
                    Event.id,
                )
                .where(
                    Event.model_id.in_(prm_ids),
                    Event.event_time >= date_from
                    if date_from is not None
                    else True,
                    Event.event_time <= date_to
                    if date_to is not None
                    else True,
                    Event.event_type.in_(
                        ["PRMCreate", "PRMUpdate", "PRMDelete"]
                    ),
                )
                .order_by(ascending)
                .limit(limit)
                .offset(offset)
            ).all()

            prm_history_total = session.execute(
                select(func.count())
                .select_from(Event)
                .where(
                    Event.model_id.in_(prm_ids),
                    Event.event_time >= date_from
                    if date_from is not None
                    else True,
                    Event.event_time <= date_to
                    if date_to is not None
                    else True,
                    Event.event_type.in_(
                        ["PRMCreate", "PRMUpdate", "PRMDelete"]
                    ),
                )
            ).scalar()

            response.append(
                {
                    "mo": mo_history,
                    "mo_params": MOParamsResponse(
                        data=prm_history, total=prm_history_total
                    ),
                }
            )
    return response


@router.get("/rebuild_geometry", response_class=StreamingResponse)
async def rebuild_geometry(
    object_type_id: int,
    session: Session = Depends(get_session),
    correct: bool = False,
):
    """Rebuild geometry based on current point_a, point_b"""
    try:
        task = RebuildGeometry(
            session=session,
            request=RebuildGeometryRequest(
                object_type_id=object_type_id, correct=correct
            ),
        )
        return await task.execute()

    except (ObjectCustomException, ObjectTypeCustomException) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/update_object_names_with_null_names")
async def update_object_names_with_null_names(
    session: Session = Depends(get_session),
):
    task = UpdateObjectNamesWithNullNames(
        session=session,
    )
    return task.execute()


@router.post("/refresh_object_names")
async def refresh_object_names(
    object_type_id: int,
    session: Session = Depends(get_session),
):
    object_type_instance = session.get(TMO, object_type_id)
    primary_tprms = object_type_instance.primary

    add_parent_name = (
        not object_type_instance.global_uniqueness and object_type_instance.p_id
    )

    query = select(MO).where(MO.tmo_id == object_type_id)
    for object_instance in session.execute(query).scalars().all():
        if primary_tprms:
            new_name_parts = []
            if add_parent_name and object_instance.p_id:
                parent_name = session.get(MO, object_instance.p_id).name
                new_name_parts.insert(0, parent_name)

            for primary_tprm in primary_tprms:
                query = select(PRM.value).where(
                    PRM.tprm_id == int(primary_tprm),
                    PRM.mo_id == object_instance.id,
                )
                parameter_value = session.execute(query).scalar()
                parameter_type_instance_val_type = session.get(
                    TPRM, int(primary_tprm)
                ).val_type
                if parameter_type_instance_val_type == "mo_link":
                    parameter_value = session.get(MO, int(parameter_value)).name
                if parameter_type_instance_val_type == "prm_link":
                    parameter_value = session.get(
                        PRM, int(parameter_value)
                    ).value

                new_name_parts.append(parameter_value)

            object_instance.name = NAME_DELIMITER.join(new_name_parts)

        else:
            object_instance.name = object_instance.id

        session.add(object_instance)

    session.commit()
    return {"status": "ok"}
