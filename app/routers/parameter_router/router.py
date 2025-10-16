import pickle
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Union, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy import or_, cast, String
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import Session
from sqlmodel import select
from starlette.background import BackgroundTasks

from common.common_exceptions import ValidationError
from common.common_schemas import ErrorResponseModel
from common.common_utils import ValueTypeValidator
from database import get_session, get_chunked_values_by_sqlalchemy_limit
from functions.db_functions.db_read import (
    get_db_object_or_exception,
    get_db_object_type_or_exception,
    get_db_param_type_or_exception_422,
    get_db_param_by_mo_and_tprm_or_exception,
    get_db_param_type_or_exception,
    get_object_out_mo_links,
    get_object_in_mo_links,
    get_db_param_or_exception,
    get_unique_parameter_type_values_by_param_type,
)
from functions.functions_dicts import (
    db_param_convert_by_val_type,
    extract_formula_parameters,
)
from functions.functions_utils.utils import (
    rename_object_when_update_primary_prm,
    decode_multiple_value,
    session_commit_create_or_exception,
    calculate_by_formula_new,
    update_mo_label_when_update_label_prm,
)
from models import (
    MO,
    TPRM,
    TMO,
    GeometryType,
)
from models import PRM, Event
from routers.object_router.exceptions import ObjectCustomException
from routers.object_router.utils import (
    update_geometry,
    validate_sequence_value_if_constraint,
)
from routers.parameter_router.exceptions import ParameterCustomException
from routers.parameter_router.processors import (
    GetParameters,
    CreateObjectParameters,
    GetParameterData,
)
from routers.parameter_router.schemas import (
    PRMCreateByMO,
    PRMUpdateByMO,
    MassiveCreateResponse,
    MassiveUpdateResponse,
    DeleteParameter,
    MassiveParameterDeleteResponse,
    PRMCreate,
    PRMUpdate,
    PRMReadMultiple,
    UpdateParameterByObject,
    CreateParameterByObject,
)
from routers.parameter_router.utils import (
    set_tmo_status_longitude_latitude_by_tprm_id_from_prm,
    update_child_prm_location,
    create_prm_for_formula,
    update_prm_for_formula,
    update_line,
    update_sequence,
    MassiveDeleteParameters,
    delete_parameter_instance,
    update_depending_sequences_after_update_constraint,
    MassiveUpdateParameters,
    MultipleCreateParameterService,
)
from routers.parameter_router.utils import (
    update_object_version_and_modification_date,
)
from val_types.constants import (
    two_way_mo_link_val_type_name,
    enum_val_type_name,
    ErrorHandlingType,
)
from val_types.enum_val_type.exceptions import EnumValTypeCustomExceptions
from val_types.enum_val_type.prm.update import EnumPRMUpdator
from val_types.two_way_mo_link_val_type.prm.create import (
    create_two_way_mo_link_prms,
)
from val_types.two_way_mo_link_val_type.prm.update import (
    update_two_way_mo_link_prms,
)

router = APIRouter(tags=["Parameters"])


@router.get("/object/{object_id}/parameters/")
async def read_object_parameters(
    object_id: int,
    session: Session = Depends(get_session),
    tprm_id: Union[List[int], None] = Query(default=None),
):
    task = GetParameters(
        session=session, object_id=object_id, parameter_type_id=tprm_id
    )
    try:
        task.check()
        return task.execute()

    except ParameterCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/object/{object_id}/parameters/", include_in_schema=False)
async def create_object_parameters(
    object_id: int,
    params: List[PRMCreateByMO],
    session: Session = Depends(get_session),
):
    task = CreateObjectParameters(
        session=session, object_id=object_id, params=params
    )
    return task.execute()


@router.patch("/object/{object_id}/parameters/", include_in_schema=False)
async def update_object_parameters(
    object_id: int,
    params: List[PRMUpdateByMO],
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session),
):
    db_mo = get_db_object_or_exception(session=session, object_id=object_id)

    result_param_list = []
    result_error_list = []

    params_by_tprm_id = defaultdict(list)
    for param in params:
        params_by_tprm_id[param.tprm_id].append(param)

    tprm_ids = set([i.tprm_id for i in params])
    tprms_stmt = select(TPRM).where(
        TPRM.id.in_(tprm_ids), TPRM.tmo_id == db_mo.tmo_id
    )
    tprms_by_val_type = defaultdict(list)
    prms_by_val_type = defaultdict(list)
    for tprm in session.execute(tprms_stmt).scalars():
        tprms_by_val_type[tprm.val_type].append(tprm)
        prms_list = params_by_tprm_id[tprm.id]
        prms_by_val_type[tprm.val_type].extend(prms_list)

    other_params = []
    for val_type, prms in prms_by_val_type.items():
        tprms = tprms_by_val_type[val_type]
        if val_type == two_way_mo_link_val_type_name:
            new_prms = {object_id: prms}
            errors, created_prms = update_two_way_mo_link_prms(
                session=session,
                update_prms=new_prms,
                in_case_of_error=ErrorHandlingType.PROCESS_CLEARED,
                autocommit=False,
                tprms=tprms,
            )
            result_param_list.extend(created_prms)
            result_error_list.extend(errors)
        if val_type == enum_val_type_name:
            update_prms = {object_id: prms}
            try:
                task = EnumPRMUpdator(
                    session=session,
                    tprm_instances=tprms,
                    parameters_by_object_id=update_prms,
                    in_case_of_error=ErrorHandlingType.PROCESS_CLEARED,
                    autocommit=False,
                )
                updated_prms_with_errors = task.update_enum_parameters()
            except EnumValTypeCustomExceptions as e:
                raise HTTPException(
                    status_code=e.status_code,
                    detail=e.detail,
                )
            result_param_list.extend(
                updated_prms_with_errors.updated_parameters
            )
            result_error_list.extend(updated_prms_with_errors.errors)
        else:
            other_params.extend(params)
    if other_params:
        error_list = []
        db_param_list = []

        db_tmo = get_db_object_type_or_exception(
            session=session, object_type_id=db_mo.tmo_id
        )

        for param in params:
            if param.value is None:
                raise HTTPException(
                    status_code=422,
                    detail="Parameter value should be not null.",
                )

            db_param = session.exec(
                select(PRM).where(
                    PRM.mo_id == object_id, PRM.tprm_id == param.tprm_id
                )
            ).first()
            if not db_param:
                raise HTTPException(
                    status_code=422,
                    detail=f"Parameter with tprm_id {param.tprm_id} not found.",
                )

            if db_param.version != param.version:
                raise HTTPException(
                    status_code=422,
                    detail=f"Actual version of PRM: {db_param.version}.",
                )

            db_param_type = get_db_param_type_or_exception_422(
                session=session, tprm_id=param.tprm_id
            )

            try:
                validation_task = ValueTypeValidator(
                    session=session,
                    parameter_type_instance=db_param_type,
                    value_to_validate=param.value,
                )
                validation_task.validate()
            except ValidationError as e:
                error_list.append({"error": e.detail})
                continue

            if db_param_type.val_type == "formula":
                try:
                    param.value = calculate_by_formula_new(
                        session=session,
                        param_type=db_param_type,
                        object_instance=db_mo,
                        x=param.value,
                    )
                except HTTPException as e:
                    error_list.append({"error": e.detail})
                    continue

            # sequence updates must be executed in the end because of constraint also can be changed
            if db_param_type.val_type == "sequence":
                update_sequence(
                    session=session,
                    new_param=param,
                    old_param=db_param,
                    param_type=db_param_type,
                )

            # update depending sequences
            query = select(TPRM).where(
                TPRM.constraint == cast(db_param_type.id, String),
                TPRM.val_type == "sequence",
            )
            depending_sequences = session.exec(query).all()
            background_tasks.add_task(
                update_depending_sequences_after_update_constraint,
                session,
                depending_sequences,
                db_param.value,
                param.value,
                object_id,
            )

            db_param.value = param.value
            db_param.version += 1
            session.add(db_param)

            if db_param.tprm_id in db_tmo.primary:
                rename_object_when_update_primary_prm(
                    session=session, db_object=db_mo, db_object_type=db_tmo
                )
            if db_param.tprm_id in db_tmo.label:
                update_mo_label_when_update_label_prm(
                    session=session, db_object=db_mo, db_object_type=db_tmo
                )
            if db_tmo.longitude == db_param.tprm_id:
                db_mo.longitude = float(db_param.value)
                session.add(db_mo)
                start_time = time.perf_counter()
                child_mo = session.exec(
                    select(MO).where(MO.p_id == db_mo.id)
                ).all()
                end_time = time.perf_counter()
                print(
                    f"Update object parameter longitude get child mo: {end_time - start_time}"
                )
                if child_mo:
                    update_child_prm_location(
                        session=session,
                        prm_name="longitude",
                        value=float(db_param.value),
                        mo_child=child_mo,
                    )
                # Check if PRM is point for line
                if not error_list:
                    update_line(session=session, db_mo=db_mo)
            if db_tmo.latitude == db_param.tprm_id:
                db_mo.latitude = float(db_param.value)
                session.add(db_mo)

                start_time = time.perf_counter()
                child_mo = session.exec(
                    select(MO).where(MO.p_id == db_mo.id)
                ).all()
                end_time = time.perf_counter()
                print(
                    f"Update object parameter latitude get child mo: {end_time - start_time}"
                )
                if child_mo:
                    update_child_prm_location(
                        session=session,
                        prm_name="latitude",
                        value=float(db_param.value),
                        mo_child=child_mo,
                    )
                # Check if PRM is point for line
                if not error_list:
                    update_line(session=session, db_mo=db_mo)
            if db_tmo.geometry_type != GeometryType.point and (
                db_tmo.longitude == db_param.tprm_id
                or db_tmo.latitude == db_param.tprm_id
            ):
                line_mos = session.exec(
                    select(MO).where(
                        or_(
                            MO.point_a_id == object_id,
                            MO.point_b_id == object_id,
                        )
                    )
                ).all()
                for line_mo in line_mos:
                    if db_mo.latitude is None or db_mo.longitude is None:
                        line_mo.geometry = {}
                        session.add(line_mo)
                    else:
                        if (
                            object_id == line_mo.point_a_id
                            and line_mo.point_b_id is not None
                        ):
                            point_b_object = session.exec(
                                select(MO).where(MO.id == line_mo.point_b_id)
                            ).first()
                            if (
                                point_b_object.latitude is not None
                                and point_b_object.longitude is not None
                            ):
                                start_time = time.perf_counter()
                                line_mo.geometry = update_geometry(
                                    object_instance=line_mo,
                                    point_a=db_mo,
                                    point_b=point_b_object,
                                )
                                end_time = time.perf_counter()
                                print(
                                    f"Update object parameter update geometry: {end_time - start_time}"
                                )
                            else:
                                line_mo.geometry = None
                            session.add(line_mo)
                        elif (
                            object_id == line_mo.point_b_id
                            and line_mo.point_a_id is not None
                        ):
                            point_a_object = session.exec(
                                select(MO).where(MO.id == line_mo.point_a_id)
                            ).first()
                            if (
                                point_a_object.latitude is not None
                                and point_a_object.longitude is not None
                            ):
                                start_time = time.perf_counter()
                                line_mo.geometry = update_geometry(
                                    object_instance=line_mo,
                                    point_a=point_a_object,
                                    point_b=db_mo,
                                )
                                end_time = time.perf_counter()
                                print(
                                    f"Update object parameter update geometry: {end_time - start_time}"
                                )
                            else:
                                line_mo.geometry = None
                            session.add(line_mo)
            if db_tmo.status == db_param.tprm_id:
                db_mo.status = db_param.value
                session.add(db_mo)

            if db_param.tprm.multiple:
                multiple_value = decode_multiple_value(db_param.value)
                param_to_read = PRMReadMultiple(
                    id=db_param.id,
                    tprm_id=db_param.tprm_id,
                    mo_id=db_param.mo_id,
                    value=multiple_value,
                    version=param.version,
                )
            else:
                param_to_read = db_param_convert_by_val_type[
                    db_param.tprm.val_type
                ](
                    db_param.id,
                    db_param.tprm_id,
                    db_param.mo_id,
                    db_param.value,
                    db_param.version,
                )

            db_param_list.append(param_to_read)

        db_mo.modification_date = datetime.utcnow()

        session.commit()
        # Check if PRM participates in the Formula
        start_time_update_formula = time.perf_counter()
        existed_tprms_names = {
            tprm.name
            for tprm in db_tmo.tprms
            if tprm.id in (prm.tprm_id for prm in db_mo.prms)
        }
        # print(f"Update object parameter formed list existed PRM: {end_time - start_time_update_formula}")
        end_time = time.perf_counter()
        print(
            f"Update object parameter create set existed TPRM name: {end_time - start_time_update_formula},"
            f" # tprm: {len(existed_tprms_names)}"
        )
        for tprm in db_tmo.tprms:
            if tprm.val_type == "formula" and tprm.constraint:
                # start_time = time.perf_counter()
                update_prm_for_formula(
                    session=session, db_param_type=tprm, mos=[db_mo]
                )
                # end_time = time.perf_counter()
                # print(f"Update object parameter get all TPRM names: {end_time - start_time}")
        end_time = time.perf_counter()
        print(
            f"Update object parameter Total time: {end_time - start_time_update_formula}"
        )
        result_error_list.extend(error_list)
        result_param_list.extend(db_param_list)
    if result_param_list:
        session.commit()
    return {"data": result_param_list, "errors": result_error_list}


@router.post(
    "/object/{object_id}/list_of_param_types/{param_type_id}/parameter/"
)
async def read_parameters(
    object_id: int,
    param_type_ids: List[int],
    session: Session = Depends(get_session),
):
    param_type_ids = set(param_type_ids)
    get_db_object_or_exception(session=session, object_id=object_id)
    prms_query = select(PRM).where(
        PRM.mo_id == object_id, PRM.tprm_id.in_(param_type_ids)
    )
    prms = session.exec(prms_query).all()
    result = []
    for db_param in prms:
        if db_param.tprm.multiple:
            multiple_value = decode_multiple_value(db_param.value)
            param_to_read = PRMReadMultiple(
                id=db_param.id,
                tprm_id=db_param.tprm_id,
                mo_id=db_param.mo_id,
                value=multiple_value,
                version=db_param.version,
            )
        else:
            param_to_read = db_param_convert_by_val_type[
                db_param.tprm.val_type
            ](
                db_param.id,
                db_param.tprm_id,
                db_param.mo_id,
                db_param.value,
                db_param.version,
            )
        result.append(param_to_read)
    return result


@router.post(
    "/object/{object_id}/param_types/{param_type_id}/parameter/",
    include_in_schema=False,
)
async def create_parameter(
    object_id: int,
    param_type_id: int,
    param: PRMCreate,
    session: Session = Depends(get_session),
):
    if param.value is None:
        raise HTTPException(
            status_code=422, detail="Parameter value should be not null."
        )

    db_object = get_db_object_or_exception(session=session, object_id=object_id)
    parameter_type_instance = get_db_param_type_or_exception(
        session=session, tprm_id=param_type_id
    )
    db_tmo = get_db_object_type_or_exception(
        session=session, object_type_id=db_object.tmo_id
    )
    if parameter_type_instance.tmo_id != db_object.tmo_id:
        raise HTTPException(
            status_code=404,
            detail="This object has no parameter type with such id.",
        )

    if parameter_type_instance == two_way_mo_link_val_type_name:
        new_prms = {
            object_id: [
                PRMCreateByMO.from_orm(param, update={"tprm_id": param_type_id})
            ]
        }
        tprms = [parameter_type_instance]
        try:
            errors, created_tprms = create_two_way_mo_link_prms(
                session=session,
                new_parameter_types=new_prms,
                parameter_types=tprms,
                in_case_of_error=ErrorHandlingType.RAISE_ERROR,
                autocommit=True,
            )
            db_param = created_tprms[0]
            update_object_version_and_modification_date(
                session=session, object_instance=db_object
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
    else:
        param = PRMCreateByMO(
            value=param.value, tprm_id=parameter_type_instance.id
        )

        validation_task = ValueTypeValidator(
            session=session,
            parameter_type_instance=parameter_type_instance,
            value_to_validate=param.value,
        )
        validation_task.validate()

        if parameter_type_instance.val_type == "formula":
            param.value = calculate_by_formula_new(
                session=session,
                param_type=parameter_type_instance,
                object_instance=db_object,
                x=param.value,
            )
        db_param = PRM(
            value=param.value, tprm_id=param_type_id, mo_id=object_id
        )
        session.add(db_param)
        session_commit_create_or_exception(
            session=session, message="This parameter value already exists."
        )
        session.refresh(db_param)

        session.info["disable_security"] = True

        set_tmo_status_longitude_latitude_by_tprm_id_from_prm(
            session=session, db_param=db_param, db_tmo=db_tmo, db_mo=db_object
        )

        update_object_version_and_modification_date(
            session=session, object_instance=db_object
        )
        if (
            db_tmo.longitude == db_param.tprm_id
            or db_tmo.latitude == db_param.tprm_id
        ):
            line_mos = session.exec(
                select(MO).where(
                    or_(MO.point_a_id == object_id, MO.point_b_id == object_id)
                )
            ).all()
            for line_mo in line_mos:
                if db_object.latitude is None or db_object.longitude is None:
                    line_mo.geometry = {}
                    session.add(line_mo)

                else:
                    if (
                        object_id == line_mo.point_a_id
                        and line_mo.point_b_id is not None
                    ):
                        point_b_object = session.exec(
                            select(MO).where(MO.id == line_mo.point_b_id)
                        ).first()
                        if (
                            point_b_object.latitude is not None
                            and point_b_object.longitude is not None
                        ):
                            line_mo.geometry = update_geometry(
                                object_instance=line_mo,
                                point_a=db_object,
                                point_b=point_b_object,
                            )
                        else:
                            line_mo.geometry = None
                        session.add(line_mo)
                    elif (
                        object_id == line_mo.point_b_id
                        and line_mo.point_a_id is not None
                    ):
                        point_a_object = session.exec(
                            select(MO).where(MO.id == line_mo.point_a_id)
                        ).first()
                        if (
                            point_a_object.latitude is not None
                            and point_a_object.longitude is not None
                        ):
                            line_mo.geometry = update_geometry(
                                object_instance=line_mo,
                                point_a=point_a_object,
                                point_b=db_object,
                            )
                        else:
                            line_mo.geometry = None
                        session.add(line_mo)

        db_object.modification_date = datetime.utcnow()
        session.commit()
        # Check if PRM participates in the Formula
        for tprm in db_tmo.tprms:
            # Try to create new PRM
            if tprm.val_type == "formula" and tprm.id not in [
                prm.tprm_id for prm in db_object.prms
            ]:
                create_prm_for_formula(
                    session=session, db_param_type=tprm, mos=[db_object]
                )
            elif tprm.val_type == "formula" and tprm.id in [
                prm.tprm_id for prm in db_object.prms
            ]:
                update_prm_for_formula(
                    session=session, db_param_type=tprm, mos=[db_object]
                )
    if db_param.tprm.multiple:
        multiple_value = decode_multiple_value(db_param.value)
        param_to_read = PRMReadMultiple(
            id=db_param.id,
            tprm_id=db_param.tprm_id,
            mo_id=db_param.mo_id,
            value=multiple_value,
            version=db_param.version,
        )
    else:
        param_to_read = db_param_convert_by_val_type[db_param.tprm.val_type](
            db_param.id,
            db_param.tprm_id,
            db_param.mo_id,
            db_param.value,
            db_param.version,
        )
    return param_to_read


@router.patch(
    "/object/{object_id}/param_types/{param_type_id}/parameter/",
    include_in_schema=False,
)
async def update_parameter(
    object_id: int,
    param_type_id: int,
    parameter: PRMUpdate,
    session: Session = Depends(get_session),
):
    if parameter.value is None:
        raise HTTPException(
            status_code=422, detail="Parameter value should be not null."
        )

    db_param = get_db_param_by_mo_and_tprm_or_exception(
        session=session, mo_id=object_id, tprm_id=param_type_id
    )
    if db_param.version != parameter.version:
        raise HTTPException(
            status_code=409,
            detail=f"Actual version of PRM: {db_param.version}.",
        )

    db_mo = get_db_object_or_exception(session=session, object_id=object_id)
    db_tmo = session.get(TMO, db_mo.tmo_id)

    parameter_type_instance = session.get(TPRM, param_type_id)

    if db_param == two_way_mo_link_val_type_name:
        update_prms = {
            object_id: [
                PRMUpdateByMO.from_orm(
                    parameter, update={"tprm_id": param_type_id}
                )
            ]
        }
        tprms = [parameter_type_instance]
        try:
            _, updated_prms = update_two_way_mo_link_prms(
                session=session,
                tprms=tprms,
                update_prms=update_prms,
                in_case_of_error=ErrorHandlingType.RAISE_ERROR,
                autocommit=True,
            )
            update_object_version_and_modification_date(
                session=session, object_instance=db_mo
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
    else:
        if (
            parameter_type_instance.val_type == "sequence"
            and parameter_type_instance.constraint
        ):
            sequence_type = session.execute(
                select(PRM.value).where(
                    PRM.tprm_id == int(parameter_type_instance.constraint),
                    PRM.mo_id == object_id,
                )
            )
            sequence_type = sequence_type.scalar()
            validate_sequence_value_if_constraint(
                session, parameter_type_instance, sequence_type, parameter.value
            )
        try:
            validation_task = ValueTypeValidator(
                session=session,
                parameter_type_instance=parameter_type_instance,
                value_to_validate=parameter.value,
            )
            validation_task.validate()
        except ValidationError as e:
            raise HTTPException(
                status_code=e.status_code,
                detail=e.detail,
            )
        if parameter_type_instance.val_type == "formula":
            parameter.value = calculate_by_formula_new(
                session=session,
                param_type=parameter_type_instance,
                object_instance=db_mo,
                x=parameter.value,
            )
        elif parameter_type_instance.val_type == "sequence":
            # sequence updates must be executed in the end because of constraint also can be changed
            update_sequence(
                session, parameter, db_param, parameter_type_instance
            )

        db_param.value = parameter.value
        db_param.version += 1
        session.add(db_param)

        if db_param.tprm_id in db_tmo.primary:
            rename_object_when_update_primary_prm(
                session=session, db_object=db_mo, db_object_type=db_tmo
            )
        if db_param.tprm_id in db_tmo.label:
            update_mo_label_when_update_label_prm(
                session=session, db_object=db_mo, db_object_type=db_tmo
            )
        set_tmo_status_longitude_latitude_by_tprm_id_from_prm(
            session=session, db_param=db_param, db_mo=db_mo, db_tmo=db_tmo
        )
        update_object_version_and_modification_date(
            session=session, object_instance=db_mo
        )
        if (
            db_tmo.longitude == db_param.tprm_id
            or db_tmo.latitude == db_param.tprm_id
        ):
            line_mos = session.exec(
                select(MO).where(
                    or_(MO.point_a_id == object_id, MO.point_b_id == object_id)
                )
            ).all()
            for line_mo in line_mos:
                if db_mo.latitude is None or db_mo.longitude is None:
                    line_mo.geometry = None
                    session.add(line_mo)

                else:
                    if (
                        object_id == line_mo.point_a_id
                        and line_mo.point_b_id is not None
                    ):
                        point_b_object = session.exec(
                            select(MO).where(MO.id == line_mo.point_b_id)
                        ).first()
                        if (
                            point_b_object.latitude is not None
                            and point_b_object.longitude is not None
                        ):
                            line_mo.geometry = update_geometry(
                                object_instance=line_mo,
                                point_a=db_mo,
                                point_b=point_b_object,
                            )
                        else:
                            line_mo.geometry = None
                        flag_modified(line_mo, "geometry")
                        session.add(line_mo)

                    elif (
                        object_id == line_mo.point_b_id
                        and line_mo.point_a_id is not None
                    ):
                        point_a_object = session.exec(
                            select(MO).where(MO.id == line_mo.point_a_id)
                        ).first()
                        if (
                            point_a_object.latitude is not None
                            and point_a_object.longitude is not None
                        ):
                            line_mo.geometry = update_geometry(
                                object_instance=line_mo,
                                point_a=point_a_object,
                                point_b=db_mo,
                            )
                        else:
                            line_mo.geometry = None
                        flag_modified(line_mo, "geometry")
                        session.add(line_mo)

        db_mo.modification_date = datetime.utcnow()
        session.commit()
        session.refresh(db_param)
    # Check if PRM participates in the Formula
    existed_tprms_names = {
        tprm.name
        for tprm in db_tmo.tprms
        if tprm.id in [prm.tprm_id for prm in db_mo.prms]
    }
    for tprm in db_tmo.tprms:
        if tprm.val_type == "formula" and tprm.constraint:
            tprm_names = extract_formula_parameters(tprm.constraint)
            if (
                tprm_names
                and parameter_type_instance.name in tprm_names
                and existed_tprms_names.issuperset(tprm_names)
            ):
                update_prm_for_formula(
                    session=session, db_param_type=tprm, mos=[db_mo]
                )
    if db_param.tprm.multiple:
        multiple_value = decode_multiple_value(db_param.value)
        param_to_read = PRMReadMultiple(
            id=db_param.id,
            tprm_id=db_param.tprm_id,
            mo_id=db_param.mo_id,
            value=multiple_value,
            version=parameter.version,
        )
    else:
        param_to_read = db_param_convert_by_val_type[db_param.tprm.val_type](
            db_param.id,
            db_param.tprm_id,
            db_param.mo_id,
            db_param.value,
            db_param.version,
        )
    return param_to_read


@router.delete("/object/{object_id}/param_types/{param_type_id}/parameter/")
async def delete_parameter(
    object_id: int, param_type_id: int, session: Session = Depends(get_session)
):
    tprm = session.get(TPRM, param_type_id)
    param = get_db_param_by_mo_and_tprm_or_exception(
        session=session, mo_id=object_id, tprm_id=param_type_id
    )
    delete_parameter_instance(tprm, session, param, object_id)
    # Object version and modification_date are updated inside delete_parameter_instance
    session.commit()
    return {"ok": True}


@router.get("/object/{object_id}/out_links/")
async def read_object_out_links(
    object_id: int, session: Session = Depends(get_session)
):
    get_db_object_or_exception(session=session, object_id=object_id)
    out_links_to_read = get_object_out_mo_links(
        session=session, object_id=object_id
    )
    return out_links_to_read


@router.get("/object/{object_id}/in_links/")
async def read_object_in_links(
    object_id: int, session: Session = Depends(get_session)
):
    db_object = get_db_object_or_exception(session=session, object_id=object_id)
    in_links_to_read = get_object_in_mo_links(
        session=session, db_object=db_object
    )
    return in_links_to_read


@router.get("/object/{object_id}/links/")
async def read_object_links(
    object_id: int, session: Session = Depends(get_session)
):
    db_object = get_db_object_or_exception(session=session, object_id=object_id)
    in_links_to_read = get_object_in_mo_links(
        session=session, db_object=db_object
    )
    out_links_to_read = get_object_out_mo_links(
        session=session, object_id=object_id
    )
    return {"in_links": in_links_to_read, "out_links": out_links_to_read}


@router.get("/param_type/{param_type_id}/unique_values/")
async def read_unique_param_type_values(
    param_type_id: int, session: Session = Depends(get_session)
):
    unique_prm_values = get_unique_parameter_type_values_by_param_type(
        session=session, param_type_id=param_type_id
    )
    if len(unique_prm_values) > 250:
        raise HTTPException(status_code=413, detail="Too much values.")
    return unique_prm_values


@router.get("/parameter/{id}/history")
async def get_parameter_history(
    id: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    session: Session = Depends(get_session),
):
    get_db_param_or_exception(session=session, prm_id=id)
    prm_history = session.exec(
        select(Event).where(
            Event.model_id == id,
            Event.event_time >= date_from if date_from is not None else True,
            Event.event_time <= date_to if date_to is not None else True,
            Event.event_type.in_(["PRMCreate", "PRMUpdate", "PRMDelete"]),
        )
    ).all()
    return prm_history


@router.get("/object/{object_id}/param_types/{param_type_id}/parameter/")
async def read_parameter(
    object_id: int, param_type_id: int, session: Session = Depends(get_session)
):
    db_param = get_db_param_by_mo_and_tprm_or_exception(
        session=session, mo_id=object_id, tprm_id=param_type_id
    )

    if db_param.tprm.multiple:
        multiple_value = decode_multiple_value(db_param.value)
        param_to_read = PRMReadMultiple(
            id=db_param.id,
            tprm_id=db_param.tprm_id,
            mo_id=db_param.mo_id,
            value=multiple_value,
            version=db_param.version,
        )
    else:
        param_to_read = db_param_convert_by_val_type[db_param.tprm.val_type](
            db_param.id,
            db_param.tprm_id,
            db_param.mo_id,
            db_param.value,
            db_param.version,
        )

    return param_to_read


@router.patch(
    path="/multiple_parameter_update",
    response_model=Union[MassiveUpdateResponse, ErrorResponseModel],
)
async def multiple_parameter_update(
    data_for_update: List[UpdateParameterByObject],
    session: Session = Depends(get_session),
):
    task = MassiveUpdateParameters(
        data_for_update=data_for_update, session=session
    )

    try:
        task.check()
        result_updated_params = task.execute()
        return {"updated_params": result_updated_params}

    except ParameterCustomException as e:
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
        )

    except EnumValTypeCustomExceptions as e:
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
        )


@router.post(
    "/multiple_parameter_delete", response_model=MassiveParameterDeleteResponse
)
async def multiple_parameter_delete(
    data_for_delete: List[DeleteParameter],
    session: Session = Depends(get_session),
):
    task = MassiveDeleteParameters(
        data_for_delete=data_for_delete, session=session
    )

    try:
        deleted_parameters = task.execute()
        return {"deleted_params": deleted_parameters}

    except ParameterCustomException as e:
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
        )


@router.post("/multiple_parameter_create", response_model=MassiveCreateResponse)
async def multiple_parameter_create(
    data_for_create: List[CreateParameterByObject],
    session: Session = Depends(get_session),
):
    task = MultipleCreateParameterService(
        data_for_create=data_for_create, session=session
    )

    try:
        task.check()
        result_created_params = task.execute()
        return {"created_params": result_created_params}

    except ParameterCustomException as e:
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
        )

    except EnumValTypeCustomExceptions as e:
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
        )

    except ObjectCustomException as e:
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
        )


@router.post("/get_full_data_about_prm_link")
async def get_full_data_about_prm_link(
    prm_id: List[int], session: Session = Depends(get_session)
):
    requested_parameters = []
    response = {}
    link_params_mappings = [
        PRM.id.label("linked_prm_id"),
        PRM.value.label("linked_prm_value"),
        MO.id.label("linked_mo_id"),
        MO.name.label("linked_mo_name"),
    ]
    for chunk in get_chunked_values_by_sqlalchemy_limit(prm_id):
        query = (
            select(PRM)
            .join(TPRM)
            .where(PRM.id.in_(chunk), TPRM.val_type == "prm_link")
        )
        requested_parameters.extend(session.execute(query).scalars().all())

    if requested_parameters:
        # get all linked prm data
        linked_prm_ids = []
        for parameter in requested_parameters:
            if parameter.tprm.multiple:
                formatted_linked_ids = pickle.loads(
                    bytes.fromhex(parameter.value)
                )
                parameter.value = formatted_linked_ids

                linked_prm_ids.extend(formatted_linked_ids)
                continue
            linked_prm_ids.append(int(parameter.value))

        # format all linked prm data
        linked_parameters_data = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(linked_prm_ids):
            query = (
                select(*link_params_mappings).join(MO).where(PRM.id.in_(chunk))
            )
            temp_resp = session.exec(query).mappings().all()
            linked_parameters_data.update(
                {
                    parameter["linked_prm_id"]: parameter
                    for parameter in temp_resp
                }
            )

        # format requested parameters
        for parameter in requested_parameters:
            formatted_response = dict(parameter)
            del formatted_response["tprm"]

            linked_parameters = []
            if isinstance(parameter.value, list):
                for linked_prm_id in parameter.value:
                    linked_parameters.append(
                        linked_parameters_data[int(linked_prm_id)]
                    )
            else:
                linked_parameters.append(
                    linked_parameters_data[int(parameter.value)]
                )

            formatted_response["linked_parameters"] = linked_parameters
            response[parameter.id] = formatted_response

    return response


@router.post("/get_full_data_about_mo_link")
async def get_full_data_about_mo_link(
    prm_id: List[int], session: Session = Depends(get_session)
):
    requested_parameters = []
    response = {}

    for chunk in get_chunked_values_by_sqlalchemy_limit(prm_id):
        query = (
            select(PRM)
            .join(TPRM)
            .where(PRM.id.in_(chunk), TPRM.val_type == "mo_link")
        )
        requested_parameters.extend(session.execute(query).scalars().all())

    if requested_parameters:
        linked_mo_ids = []

        for parameter in requested_parameters:
            if parameter.tprm.multiple:
                formatted_linked_ids = pickle.loads(
                    bytes.fromhex(parameter.value)
                )
                parameter.value = formatted_linked_ids
                linked_mo_ids.extend(formatted_linked_ids)
                continue

            linked_mo_ids.append(int(parameter.value))

        linked_objects_data = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(linked_mo_ids):
            query = select(
                MO.id.label("linked_mo_id"), MO.name.label("linked_mo_name")
            ).where(MO.id.in_(chunk))
            temp_resp = session.exec(query).mappings().all()
            linked_objects_data.update(
                {
                    parameter["linked_mo_id"]: parameter
                    for parameter in temp_resp
                }
            )

        for parameter in requested_parameters:
            formatted_response = dict(parameter)
            del formatted_response["tprm"]

            linked_objects = []
            if isinstance(parameter.value, list):
                for linked_mo_id in parameter.value:
                    linked_objects.append(
                        linked_objects_data[int(linked_mo_id)]
                    )
            else:
                linked_objects.append(linked_objects_data[int(parameter.value)])

            formatted_response["linked_objects"] = linked_objects
            response[parameter.id] = formatted_response
    return response


@router.post("/get_full_data_about_two_way_mo_link")
async def get_full_data_about_two_way_mo_link(
    prm_id: List[int], session: Session = Depends(get_session)
):
    requested_parameters = []
    response = {}

    for chunk in get_chunked_values_by_sqlalchemy_limit(prm_id):
        query = (
            select(PRM)
            .join(TPRM)
            .where(
                PRM.id.in_(chunk),
                TPRM.val_type == two_way_mo_link_val_type_name,
            )
        )
        requested_parameters.extend(session.execute(query).scalars().all())

    if requested_parameters:
        linked_mo_ids = [
            int(parameter.value) for parameter in requested_parameters
        ]

        linked_objects_data = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(linked_mo_ids):
            query = select(
                MO.id.label("linked_mo_id"), MO.name.label("linked_mo_name")
            ).where(MO.id.in_(chunk))
            temp_resp = session.exec(query).mappings().all()
            linked_objects_data.update(
                {
                    parameter["linked_mo_id"]: parameter
                    for parameter in temp_resp
                }
            )

        for parameter in requested_parameters:
            formatted_response = dict(parameter)

            formatted_response["linked_object"] = linked_objects_data[
                int(parameter.value)
            ]

            response[parameter.id] = formatted_response
    return response


@router.post("/get_parameter_data")
async def get_parameter_data(
    parameter_ids: list[int], session: Session = Depends(get_session)
):
    task = GetParameterData(
        session=session,
        parameter_ids=parameter_ids,
    )
    return task.execute()
