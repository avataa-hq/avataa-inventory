from collections import defaultdict
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from sqlmodel import Session, select

from database import get_session
from functions.db_functions.db_add import (
    add_required_params_for_objects_when_update_param_type,
)
from functions.db_functions.db_create import create_param_types_with_error_list
from functions.db_functions.db_read import (
    get_db_object_type_or_exception,
    get_db_param_type_or_exception,
    get_db_param_type_and_prms_or_exception,
)
from functions.db_functions.db_update import (
    update_prm_links_by_tprm_id,
    update_object_type_if_delete_longitude_or_latitude_or_status,
)
from functions.functions_utils.utils import (
    replace_constraint_prm_link,
    session_commit_create_or_exception,
)
from functions.validation_functions.validation_function import (
    force_field_validation_when_update_val_type,
    params_validation_and_convertation_when_change_val_type,
)
from models import (
    TPRM,
    TMO,
    Event,
    PRM,
)
from routers.object_router.utils import filter_flags_by_tprm
from routers.object_type_router.exceptions import ObjectTypeCustomException
from routers.parameter_router.utils import (
    update_param_validation,
    update_prm_for_formula,
)
from routers.parameter_type_router.constants import RESERVED_NAMES_FOR_TPRMS
from routers.parameter_type_router.exceptions import (
    ParameterTypeCustomException,
)
from routers.parameter_type_router.processors import CreateParameterType
from routers.parameter_type_router.schemas import (
    TPRMResponse,
    TmoTprmsCreationResponse,
    TPRMUpdateWithValType,
    TPRMReadWithPrimary,
    TPRMCreate,
    TPRMUpdate,
    TPRMCreateByTMO,
    TPRMUpdateValtype,
)
from routers.parameter_type_router.utils import (
    build_sequence,
    update_sequences_after_constraint_delete,
)
from routers.parameter_type_router.utils import (
    validate_param_type_before_update,
    validate_param_type_before_create,
    validate_param_type_before_change_type,
)
from val_types.constants import (
    enum_val_type_name,
    ErrorHandlingType,
    two_way_mo_link_val_type_name,
)
from val_types.enum_val_type.exceptions import EnumValTypeCustomExceptions
from val_types.enum_val_type.tprm.create import EnumTPRMCreator
from val_types.enum_val_type.tprm.delete import EnumTPRMDeleter
from val_types.enum_val_type.tprm.update import EnumTPRMUpdater
from val_types.two_way_mo_link_val_type.tprm.create import (
    create_two_way_mo_link_tprms,
)
from val_types.two_way_mo_link_val_type.tprm.delete import (
    delete_two_way_mo_link_tprms,
)
from val_types.two_way_mo_link_val_type.tprm.update import (
    update_two_way_mo_link_tprms,
)

router = APIRouter(tags=["Parameter types"])


@router.get("/param_types/", response_model=List[TPRMResponse])
async def read_param_types(session: Session = Depends(get_session)):
    param_types = session.exec(select(TPRM)).all()
    replace_constraint_prm_link(session, param_types)
    return param_types


@router.post("/param_type/", response_model=TPRMResponse)
async def create_param_type(
    param_type: TPRMCreate,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session),
):
    try:
        task = CreateParameterType(
            request=param_type,
            session=session,
            background_tasks=background_tasks,
        )
        return task.execute()

    except (ParameterTypeCustomException, ObjectTypeCustomException) as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/param_type/{id}", response_model=TPRMResponse)
async def read_param_type(id: int, session: Session = Depends(get_session)):
    param_type = get_db_param_type_or_exception(session=session, tprm_id=id)
    replace_constraint_prm_link(session, param_type)
    return param_type


@router.patch("/param_type/{id}", response_model=TPRMResponse)
async def update_param_type(
    id: int, param_type: TPRMUpdate, session: Session = Depends(get_session)
):
    db_param_type = get_db_param_type_or_exception(session=session, tprm_id=id)
    object_type_instance = get_db_object_type_or_exception(
        session=session, object_type_id=db_param_type.tmo_id
    )

    if param_type.name and "." in param_type.name:
        raise HTTPException(
            status_code=422,
            detail="TPRM name can't contains dot symbol.",
        )

    if db_param_type.val_type == two_way_mo_link_val_type_name:
        try:
            _, created_tprms = update_two_way_mo_link_tprms(
                session=session,
                update_tprms={id: param_type},
                in_case_of_error=ErrorHandlingType.RAISE_ERROR,
            )
            return created_tprms[0]
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    elif db_param_type.val_type.lower() == enum_val_type_name:
        try:
            task = EnumTPRMUpdater(
                session=session, object_type_instance=object_type_instance
            )
            updated_tprms_and_errors = task.update_param_types(
                param_types_for_update={id: param_type}
            )
            return updated_tprms_and_errors.updated_param_types[0]

        except EnumValTypeCustomExceptions as e:
            raise HTTPException(
                status_code=e.status_code,
                detail=e.detail,
            )
    else:
        validation_res = validate_param_type_before_update(
            session, db_param_type, param_type
        )
        param_type_data = validation_res["param_type_data"]
        field_value = validation_res["field_value"]

        old_constraint = db_param_type.constraint

        for key, value in param_type_data.items():
            setattr(db_param_type, key, value)
        db_param_type.modification_date = datetime.utcnow()
        session.add(db_param_type)
        session_commit_create_or_exception(
            session=session, message="This parameter type already exists."
        )
        session.refresh(db_param_type)
        if param_type.required:
            add_required_params_for_objects_when_update_param_type(
                session=session,
                db_param_type=db_param_type,
                field_value=field_value,
            )

        # rebuild sequence if constraint changed
        if (
            db_param_type.val_type == "sequence"
            and old_constraint != param_type.constraint
        ):
            parameter_for_delete = session.exec(
                select(PRM).where(PRM.tprm_id == db_param_type.id)
            ).first()
            session.delete(parameter_for_delete)
            build_sequence(session=session, tprm=db_param_type)

        session.commit()
        session.refresh(db_param_type)
        return db_param_type


@router.delete("/param_type/{id}")
async def delete_param_type(id: int, session: Session = Depends(get_session)):
    param_type = get_db_param_type_or_exception(session=session, tprm_id=id)
    db_object_type = session.get(TMO, param_type.tmo_id)
    if param_type.val_type == two_way_mo_link_val_type_name:
        try:
            delete_two_way_mo_link_tprms(
                session=session,
                tprm_ids=[id],
                in_case_of_error=ErrorHandlingType.RAISE_ERROR,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
    elif param_type.val_type == enum_val_type_name:
        try:
            task = EnumTPRMDeleter(
                session=session,
                param_types_to_delete=[param_type],
                object_type_instance=db_object_type,
            )
            task.delete_enum_tprms()

        except EnumValTypeCustomExceptions as e:
            raise HTTPException(status_code=422, detail=str(e))
    else:
        session.info["disable_security"] = True
        if id in db_object_type.primary:
            raise HTTPException(
                status_code=409,
                detail="Unable to delete. Param type is primary for object type.",
            )
        if id in db_object_type.label:
            raise HTTPException(
                status_code=409,
                detail="Unable to delete. Param type is label for object type.",
            )
        # Check if current tprm has tprms linked to him
        tprms_with_link_to_current_tprm = session.exec(
            select(TPRM).where(
                TPRM.val_type.in_(["prm_link", "sequence"]),
                TPRM.constraint == str(id),
            )
        ).all()
        if tprms_with_link_to_current_tprm:
            update_prm_links_by_tprm_id(
                session=session,
                current_tprm=param_type,
                linked_tprms=[
                    tprm
                    for tprm in tprms_with_link_to_current_tprm
                    if tprm.val_type == "prm_link"
                ],
            )
            update_sequences_after_constraint_delete(
                session=session,
                linked_tprms=[
                    tprm
                    for tprm in tprms_with_link_to_current_tprm
                    if tprm.val_type == "sequence"
                ],
            )

        update_object_type_if_delete_longitude_or_latitude_or_status(
            session=session, param_type=param_type
        )
        session.delete(param_type)
        session.commit()
    return {"ok": True}


@router.get(
    "/object_type/{id}/param_types/", response_model=List[TPRMReadWithPrimary]
)
async def read_object_type_param_types(
    id: int,
    group: Optional[str] = None,
    tprm_ids: Optional[list] = Query(None),
    session: Session = Depends(get_session),
):
    db_tmo = get_db_object_type_or_exception(session=session, object_type_id=id)

    where_condition = []
    if group:
        where_condition.append(TPRM.group == group)

    if tprm_ids:
        try:
            tprm_ids = [int(x) for x in tprm_ids]
        except BaseException as e:
            raise HTTPException(status_code=422, detail=str(e))

        where_condition.append(TPRM.id.in_(tprm_ids))

    param_types = session.exec(
        select(TPRM).where(TPRM.tmo_id == id, *where_condition)
    ).all()
    replace_constraint_prm_link(session, param_types)

    if tprm_ids:
        param_dict = {param.id: param for param in param_types}
        param_types = [
            param
            for param_id in tprm_ids
            if (param := param_dict.get(param_id))
        ]

    param_types_to_read = []
    for param_type in param_types:
        tprm_to_read = TPRMReadWithPrimary.from_orm(param_type)
        if param_type.id in db_tmo.primary:
            tprm_to_read.primary = True
        param_types_to_read.append(tprm_to_read)
    return param_types_to_read


@router.post(
    "/object_type/{id}/param_types/", response_model=TmoTprmsCreationResponse
)
async def create_object_type_param_types(
    id: int,
    param_types: List[TPRMCreateByTMO],
    session: Session = Depends(get_session),
):
    object_type_instance = get_db_object_type_or_exception(
        session=session, object_type_id=id
    )
    param_types_by_val_types = defaultdict(list)
    for val_type in param_types:
        param_types_by_val_types[val_type.val_type].append(val_type)
    result_errors = []
    result_created_tprms = []
    for val_type, param_types_list in param_types_by_val_types.items():
        if val_type == two_way_mo_link_val_type_name:
            errors, created_tprms = create_two_way_mo_link_tprms(
                session=session,
                new_tprms=param_types_list,
                in_case_of_error=ErrorHandlingType.PROCESS_CLEARED,
                autocommit=True,
            )
            result_errors.extend(errors)
            result_created_tprms.extend(created_tprms)
        elif val_type.lower() == enum_val_type_name:
            try:
                task = EnumTPRMCreator(
                    session=session, object_type_instance=object_type_instance
                )
                created_tprms_and_errors = task.create_enum_tprms(
                    new_param_types=param_types_list
                )
                result_errors.extend(created_tprms_and_errors.errors)
                result_created_tprms.extend(
                    created_tprms_and_errors.created_param_types
                )

            except EnumValTypeCustomExceptions as e:
                raise HTTPException(
                    status_code=e.status_code,
                    detail=e.detail,
                )
        else:
            param_types_list, error_list = create_param_types_with_error_list(
                session=session,
                param_types=param_types,
                tmo=object_type_instance,
            )
            result_errors.extend(error_list)
            result_created_tprms.extend(param_types_list)
    if len(result_created_tprms) == 0:
        raise HTTPException(status_code=409, detail=result_errors)

    session.commit()

    return TmoTprmsCreationResponse(
        data=result_created_tprms, errors=result_errors
    )


@router.patch("/param_type/{id}/change_val_type/", response_model=TPRMResponse)
async def update_val_type(
    id: int,
    param_type: TPRMUpdateValtype,
    session: Session = Depends(get_session),
):
    db_param_type = get_db_param_type_and_prms_or_exception(
        session=session, tprm_id=id
    )
    validate_param_type_before_change_type(
        session=session, db_param_type=db_param_type, param_type=param_type
    )

    if update_param_validation(
        session=session, db_param_type=db_param_type, param_type=param_type
    ):
        db_param_type.constraint = None
        db_param_type.version += 1
        db_param_type.modification_date = datetime.utcnow()
        db_param_type.field_value = param_type.field_value
        session.add(db_param_type)

        force_field_validation_when_update_val_type(
            param_type=param_type, db_param_type=db_param_type
        )
        params_validation_and_convertation_when_change_val_type(
            session=session, param_type=param_type, db_param_type=db_param_type
        )

        db_param_type.val_type = param_type.val_type
        session.commit()
        session.refresh(db_param_type)
        return db_param_type


@router.get(
    "/object_type/{id}/required_param_types/",
    response_model=List[TPRMReadWithPrimary],
)
async def read_required_param_types(
    id: int, session: Session = Depends(get_session)
):
    db_tmo = get_db_object_type_or_exception(session=session, object_type_id=id)
    param_types = session.exec(
        select(TPRM).where(TPRM.tmo_id == id, TPRM.required == True)  # noqa
    ).all()
    param_types_to_read = []
    for param_type in param_types:
        tprm_to_read = TPRMReadWithPrimary.from_orm(param_type)
        if param_type.id in db_tmo.primary:
            tprm_to_read.primary = True
        param_types_to_read.append(tprm_to_read)
    replace_constraint_prm_link(session, param_types_to_read)
    return param_types_to_read


@router.get("/search_param_types/", response_model=List[TPRMResponse])
async def search_param_types(
    name: str, session: Session = Depends(get_session)
):
    param_types = session.exec(
        select(TPRM).where(TPRM.name.like("%" + name + "%"))
    ).all()
    replace_constraint_prm_link(session, param_types)
    return param_types


@router.get("/param_type/{id}/history")
async def get_param_type_history(
    id: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    session: Session = Depends(get_session),
):
    get_db_param_type_or_exception(session=session, tprm_id=id)
    tprm_history = session.exec(
        select(Event).where(
            Event.model_id == id,
            Event.event_time >= date_from if date_from is not None else True,
            Event.event_time <= date_to if date_to is not None else True,
            Event.event_type.in_(["TPRMCreate", "TPRMUpdate", "TPRMDelete"]),
        )
    ).all()
    return tprm_history


@router.get("/param_types/{obj_type_id}/filter_flags")
async def get_object_type_filter_flags(
    obj_type_id: int, session: Session = Depends(get_session)
):
    """Returns dict of param_type ids and filter flags"""

    stmt = select(TPRM).where(
        TPRM.tmo_id == obj_type_id,
        TPRM.multiple == False,  # noqa
    )
    tprms = session.exec(stmt).all()

    return {tprm.id: filter_flags_by_tprm(tprm) for tprm in tprms}


@router.post("/param_types/{obj_type_id}/batch_create_or_update_param_types")
async def batch_create_or_update_param_types(
    obj_type_id: int,
    param_types: List[TPRMCreateByTMO],
    check: bool = Query(default=True),
    session: Session = Depends(get_session),
):
    object_type_instance = get_db_object_type_or_exception(
        session=session, object_type_id=obj_type_id
    )
    tprms_from_request = {}
    for tprm in param_types:
        if tprm.name in RESERVED_NAMES_FOR_TPRMS:
            raise HTTPException(
                status_code=422,
                detail=f"There is name for TPRM, which can't be use, because names: {RESERVED_NAMES_FOR_TPRMS} "
                f"are reserved.",
            )
        if "." in tprm.name:
            raise HTTPException(
                status_code=422,
                detail="TPRM name can't contains dot symbol.",
            )
        tprms_from_request[tprm.name] = tprm

    if len(tprms_from_request) != len(set(tprms_from_request)):
        raise HTTPException(
            status_code=400, detail="All TPRMs names must be unique!"
        )

    stmt = select(TPRM).where(
        TPRM.tmo_id == obj_type_id, TPRM.name.in_(tprms_from_request)
    )
    tprms_from_db = session.exec(stmt).all()

    tprm_to_update = []

    for tprm in tprms_from_db:
        tprm_data_from_request = tprms_from_request[tprm.name].dict(
            exclude_unset=True
        )

        val_type = tprm_data_from_request.get("val_type")
        if val_type and val_type != tprm.val_type:
            raise HTTPException(
                status_code=422,
                detail="You can`t change val_type at this endpoint.",
            )

        equals = True

        for k, v in tprm_data_from_request.items():
            if getattr(tprm, k) != v:
                equals = False
                break

        if equals is False:
            if (
                tprm.val_type == two_way_mo_link_val_type_name
                or tprm.val_type == enum_val_type_name
            ):
                tprm_to_update.append(
                    {
                        "db_param_type": tprm,
                        "param_type": tprms_from_request[tprm.name],
                    }
                )
            else:
                data = tprm.dict()
                data.update(tprm_data_from_request)
                tprm_with_updated_data = TPRMUpdateWithValType(**data)

                tprm_to_update.append(
                    {
                        "db_param_type": tprm,
                        "param_type": tprm_with_updated_data,
                    }
                )

    # validate all tprm to update
    updated_tprms = []
    process_case = (
        ErrorHandlingType.ONLY_CHECKING
        if check
        else ErrorHandlingType.RAISE_ERROR
    )
    for item in tprm_to_update:
        db_param_type = item["db_param_type"]
        param_type = item["param_type"]
        if db_param_type.val_type == two_way_mo_link_val_type_name:
            update_tprms = {db_param_type.id: [param_type]}
            errors, upd_tprms = update_two_way_mo_link_tprms(
                session=session,
                update_tprms=update_tprms,
                in_case_of_error=process_case,
            )  # noqa
            updated_tprms.extend(upd_tprms)

        if db_param_type.val_type == enum_val_type_name:
            param_type_to_update = param_type.dict(exclude_unset=True)
            param_type_to_update["version"] = db_param_type.version
            param_type_to_update["force"] = True
            param_type_to_update = TPRMUpdate(**param_type_to_update)
            update_tprms = {db_param_type.id: param_type_to_update}
            try:
                task = EnumTPRMUpdater(
                    session=session,
                    object_type_instance=object_type_instance,
                    in_case_of_error=process_case,
                    autocommit=False,
                )
                updated_tprms_with_errors = task.update_param_types(
                    param_types_for_update=update_tprms
                )

            except EnumValTypeCustomExceptions as e:
                raise HTTPException(
                    status_code=e.status_code,
                    detail=e.detail,
                )

            updated_tprms.extend(updated_tprms_with_errors.updated_param_types)

        else:
            validation_res = validate_param_type_before_update(
                session, db_param_type, param_type
            )

            if check:
                continue
            else:
                param_type_data = validation_res["param_type_data"]
                field_value = validation_res["field_value"]

                for key, value in param_type_data.items():
                    setattr(db_param_type, key, value)
                db_param_type.modification_date = datetime.utcnow()
                db_param_type.version += 1
                session.add(db_param_type)
                session.flush()
                updated_tprms.append(db_param_type)
                if not db_param_type.required and param_type.required:
                    add_required_params_for_objects_when_update_param_type(
                        session=session,
                        db_param_type=db_param_type,
                        field_value=field_value,
                    )

    # create
    if tprms_from_db:
        names_to_update = [tprm.name for tprm in tprms_from_db]
        tprm_from_request_to_create = [
            tprm for tprm in param_types if tprm.name not in names_to_update
        ]
    else:
        tprm_from_request_to_create = param_types

    tprm_by_val_type = defaultdict(list)
    for tprm in tprm_from_request_to_create:
        tprm_by_val_type[tprm.val_type].append(tprm)

    other_types = []
    created_tprms = []
    errors_list = []
    for val_type, val_type_tprms in tprm_by_val_type.items():
        if val_type == two_way_mo_link_val_type_name:
            errors, crt_tprms = create_two_way_mo_link_tprms(
                session=session,
                new_tprms=val_type_tprms,
                autocommit=False,
                in_case_of_error=process_case,
            )
            errors_list.extend(errors)
            created_tprms.extend(crt_tprms)
        elif val_type == enum_val_type_name:
            task = EnumTPRMCreator(
                session=session,
                object_type_instance=object_type_instance,
                in_case_of_error=process_case,
                autocommit=False,
            )

            created_tprms_with_errors = task.create_enum_tprms(
                new_param_types=val_type_tprms
            )
            errors_list.extend(created_tprms_with_errors.errors)
            created_tprms.extend(created_tprms_with_errors.created_param_types)
        else:
            # validate before create
            error_list = validate_param_type_before_create(
                session, tprm_from_request_to_create, obj_type_id
            )
            errors_list.extend(error_list)
            other_types.extend(tprm_from_request_to_create)
    if errors_list:
        raise HTTPException(status_code=400, detail=errors_list)

    if other_types:
        if check is False:
            other_types = sorted(
                other_types, key=lambda x: x.val_type == "sequence"
            )
            for param_type in other_types:
                db_param_type = TPRM(
                    name=param_type.name,
                    val_type=param_type.val_type,
                    multiple=param_type.multiple,
                    created_by="",
                    modified_by="",
                    required=param_type.required,
                    returnable=param_type.returnable,
                    group=param_type.group,
                    constraint=param_type.constraint,
                    prm_link_filter=param_type.prm_link_filter
                    if param_type.val_type == "prm_link"
                    else None,
                    tmo_id=obj_type_id,
                )
                session.add(db_param_type)
                session.flush()
                created_tprms.append(db_param_type)
                if db_param_type.val_type == "sequence":
                    build_sequence(session=session, tprm=db_param_type)

    res = {}
    if check:
        res["will_be_updated"] = len(tprm_to_update)
        res["will_be_created"] = len(tprm_from_request_to_create)
    else:
        res = updated_tprms + created_tprms
        for x in res:
            session.refresh(x)
        res = [TPRMResponse.from_orm(i) for i in res]
    session.commit()

    return res


@router.post("/param_type/{tprm_id}/recalc_formula/")
async def recalc_formula(tprm_id: int, session: Session = Depends(get_session)):
    db_param_type: TPRM = get_db_param_type_or_exception(
        session=session, tprm_id=tprm_id
    )
    if db_param_type.val_type == "formula":
        update_prm_for_formula(session=session, db_param_type=db_param_type)
        return {"status": "ok"}
    raise HTTPException(
        status_code=422,
        detail="Please specify the correct value for the parameter with a formula type.",
    )
