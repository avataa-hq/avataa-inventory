import copy
import datetime
import pickle
from typing import List, Union, Tuple

import sqlalchemy
from fastapi import HTTPException
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select, Session

from common.common_constant import (
    allowed_val_types,
    not_required_val_types,
    not_multiple_val_types,
    val_types_with_required_constraint,
)
from common.common_exceptions import ValidationError
from common.common_utils import ValueTypeValidator
from functions import functions_dicts
from functions.db_functions import db_add, db_read
from functions.functions_utils.utils import (
    calculate_by_formula_new,
    set_param_attrs,
)
from functions.validation_functions.validation_function import (
    get_param_to_read_based_on_multiple,
)
from models import TMO, MO, TPRM, PRM
from routers.object_router import utils
from routers.object_router.exceptions import (
    ObjectNotExists,
    ParentNotMatchToObjectTypeParent,
)
from routers.object_router.schemas import MOCreateWithParams
from routers.object_type_router.exceptions import (
    ObjectTypeHasNoParent,
    ObjectTypeIsVirtual,
)
from routers.object_type_router.schemas import TMOCreate
from routers.object_type_router.utils import (
    validate_lifecycle_process_definition,
)
from routers.parameter_router import utils as param_utils
from routers.parameter_router.schemas import PRMCreateByMO
from routers.parameter_type_router.constants import RESERVED_NAMES_FOR_TPRMS
from routers.parameter_type_router.schemas import TPRMCreateByTMO
from routers.parameter_type_router.utils import (
    get_tprm_by_tmo_and_name,
    build_sequence,
)
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)


def create_db_object_type(session: Session, object_type: TMOCreate) -> TMO:
    if object_type.p_id is not None:
        db_parent_object_type = session.get(TMO, object_type.p_id)
        if not db_parent_object_type:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid parent id. Object with id {object_type.p_id} not found.",
            )
    if object_type.lifecycle_process_definition is not None:
        validate_lifecycle_process_definition(
            object_type.lifecycle_process_definition
        )

    # 'points_constraint_by_tmo' stores list of tmo ids, and feature point_a\b have to be in these TMO`s
    if object_type.points_constraint_by_tmo:
        stmt = select(TMO.id).where(
            TMO.id.in_(object_type.points_constraint_by_tmo)
        )
        exists_tmos = session.execute(stmt).scalars().all()
        requested_tmos_as_constraint = set(object_type.points_constraint_by_tmo)
        if exists_tmos is None:
            exists_tmos = []
        # if there TMO`s which are not exists -- we need to raise error
        differance = requested_tmos_as_constraint.difference(set(exists_tmos))
        if differance:
            raise HTTPException(
                status_code=422,
                detail="There are TMO`s, which can't be used by constraint, because of they are not "
                f"exists: {differance}",
            )

    object_type = object_type.dict()
    object_type["created_by"] = get_username_from_session(session=session)
    object_type["modified_by"] = get_username_from_session(session=session)
    db_object_type = TMO(**object_type)
    return db_object_type


def create_db_object(
    session: Session, object_to_update: MOCreateWithParams, object_type: TMO
) -> MO:
    object_to_update = dict(object_to_update)
    object_to_update = MO(**object_to_update)
    if object_to_update.p_id is not None:
        query = select(MO).where(MO.id == object_to_update.p_id)
        parent_object = session.exec(query).first()

        if not parent_object:
            raise ObjectNotExists(
                status_code=422,
                detail=f"Invalid parent id ({object_to_update.p_id}). Object does not exist.",
            )
        if object_type.p_id is None:
            raise ObjectTypeHasNoParent(
                status_code=422,
                detail=f"Its impossible to create parent link, because TMO "
                f"with id {object_type.id} has no parent",
            )
        if object_type.p_id != parent_object.tmo_id:
            raise ParentNotMatchToObjectTypeParent(
                status_code=422,
                detail=f"Invalid parent id ({object_to_update.p_id})."
                f" Parent should be object of object type"
                f" with id {object_type.p_id}.",
            )

    if object_type.virtual:
        raise ObjectTypeIsVirtual(
            status_code=422,
            detail="Unable to create object of virtual object type.",
        )

    if object_to_update.point_a_id is not None:
        db_read.get_object_by_point_a_id_or_exception(
            session=session, db_object_id=object_to_update.point_a_id
        )

    if object_to_update.point_b_id is not None:
        db_read.get_object_by_point_b_id_or_exception(
            session=session, db_object_id=object_to_update.point_b_id
        )
    if object_to_update.geometry is None:
        object_to_update.geometry = sqlalchemy.null()

    return object_to_update


def create_param_types_with_error_list(
    session: Session, param_types: List[TPRMCreateByTMO], tmo: TMO
) -> Tuple:
    param_types_list = []
    error_list = []
    tmo_id = tmo.id
    param_types = sorted(param_types, key=lambda x: x.val_type == "sequence")

    for param_type in param_types:
        if param_type.name in RESERVED_NAMES_FOR_TPRMS:
            error_list.append(
                {
                    "error": f"There is name for TPRM, which can't be use, because names: {RESERVED_NAMES_FOR_TPRMS} "
                    f"are reserved."
                }
            )
            continue

        if get_tprm_by_tmo_and_name(session, tmo_id, param_type.name):
            error_list.append(
                {
                    "error": f"Parameter type with name: '{param_type.name}' "
                    f"already exists in this object type!"
                }
            )
            continue

        if param_type.val_type.lower() in allowed_val_types:
            param_type.val_type = param_type.val_type.lower()
        else:
            error_list.append(
                {
                    "error": f"Incorrect valtype. Allowed valtypes: {allowed_val_types}."
                }
            )
            continue
        if (
            param_type.val_type in not_multiple_val_types
            and param_type.multiple
        ):
            error_list.append(
                {
                    "error": f"Not allowed to create multiple parameter for {param_type.val_type} val_type."
                }
            )
            continue
        if (
            param_type.val_type in not_required_val_types
            and param_type.required
        ):
            error_list.append(
                {
                    "error": f"Not allowed to create required parameter for {param_type.val_type} val_type."
                }
            )
            continue
        if param_type.constraint:
            if not functions_dicts.error_param_type_constraint_validation[
                param_type.val_type
            ](param_type, tmo.id, error_list, session):
                continue
        else:
            if param_type.val_type in val_types_with_required_constraint:
                error_list.append(
                    {
                        "error": f"Please, pass the constraint parameter. "
                        f"It is required for {param_type.val_type} val_type."
                    }
                )
                continue
        param_type_data = param_type.dict(exclude_unset=True)

        if "field_value" in param_type_data:
            if param_type_data["field_value"] is None and param_type.required:
                error_list.append(
                    {
                        "error": "Please, pass the 'field_value' when creating required parameter type."
                    }
                )
                continue

        elif param_type.required:
            error_list.append(
                {
                    "error": "Please, pass the 'field_value' when creating required parameter type."
                }
            )
            continue

        if param_type.required:
            field_value = param_type_data["field_value"]

            try:
                validation_task = ValueTypeValidator(
                    session=session,
                    parameter_type_instance=param_type,
                    value_to_validate=field_value,
                )
                field_value = validation_task.validate()

            except ValidationError as e:
                error_list.append({"error": e.detail})
                continue

            if param_type.multiple:
                field_value = pickle.dumps(field_value).hex()

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
            tmo_id=tmo.id,
        )
        session.add(db_param_type)
        try:
            session.flush()
            session.refresh(db_param_type)
        except sqlalchemy.exc.IntegrityError:
            error_list.append(
                {
                    "error": f"Parameter type with name '{param_type.name}' and tmo_id {tmo_id} already exists."
                }
            )
            continue
        session.refresh(db_param_type)
        # because of db_param_type uses by session -- after session commit it disappears
        # so as mutable object it changes to empty
        copy_of_db_param_type = copy.copy(db_param_type)
        param_types_list.append(copy_of_db_param_type)

        if db_param_type.required:
            db_add.add_required_params_for_objects_when_create_param_type(
                session=session,
                db_param_type=db_param_type,
                field_value=field_value,
            )

        if db_param_type.val_type == "sequence":
            build_sequence(session=session, tprm=db_param_type)

    return param_types_list, error_list


def create_parameters_with_error_list(
    session: Session, params: List[PRMCreateByMO], db_object: MO
) -> Union[list, list]:
    db_param_list = []
    error_list = []

    all_tprm_id_from_params = [param.tprm_id for param in params]

    # TODO check for correct error message
    if session.exec(
        select(PRM).where(
            PRM.tprm_id.in_(all_tprm_id_from_params), PRM.mo_id == db_object.id
        )
    ).all():
        raise HTTPException(
            status_code=422,
            detail=f"Parameter with TPRM id {params[0].tprm_id} and object id "
            f"{db_object.id} already exists",
        )
    db_tmo = db_read.get_db_object_type_or_exception(
        session=session, object_type_id=db_object.tmo_id
    )
    tmo_longitude = db_tmo.longitude
    tmo_latitude = db_tmo.latitude
    tmo_status = db_tmo.status

    for param in params:
        if param.value is None:
            error_list.append({"error": "No parameter value passed."})
            continue
        db_param_type = db_read.get_db_param_type_or_exception(
            session=session, tprm_id=param.tprm_id
        )

        if not db_param_type:
            error_list.append(
                {
                    "error": f"Parameter type with id {param.tprm_id} doesn't exist."
                }
            )
            continue
        if db_param_type.tmo_id != db_object.tmo_id:
            error_list.append(
                {
                    "error": f"This object has no parameter type with such id ({db_param_type.id})."
                }
            )
            continue

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

        if db_param_type.multiple:
            param.value = pickle.dumps(param.value).hex()

        if db_param_type.val_type == "formula":
            try:
                param.value = calculate_by_formula_new(
                    session=session,
                    param_type=db_param_type,
                    object_instance=db_object,
                    x=param.value,
                )
            except SyntaxError as ex:
                error_list.append({"error": ex.msg})
                continue
            except ValueError as ex:
                error_list.append({"error": ex.args})
                continue
            except HTTPException as e:
                error_list.append({"error": e.detail})
                continue
        db_param = PRM(
            tprm_id=param.tprm_id, mo_id=db_object.id, value=param.value
        )
        session.add(db_param)
        try:
            db_object.modification_date = datetime.datetime.utcnow()
            db_object.version += 1
            session.flush()
            session.refresh(db_param)

            attribute_list = {
                "longitude": tmo_longitude,
                "latitude": tmo_latitude,
                "status": tmo_status,
            }

            for attribute in attribute_list:
                set_param_attrs(
                    session=session,
                    db_param=db_param,
                    attr_value=attribute,
                    attribute=attribute_list[attribute],
                    db_object=db_object,
                )

            session.flush()
        except sqlalchemy.exc.IntegrityError:
            error_list.append(
                {
                    "error": f"Parameter with tprm_id {param.tprm_id} and mo_id {db_object.id} already exists."
                }
            )
            continue
        session.refresh(db_param)

        db_param_list.append(
            get_param_to_read_based_on_multiple(db_param=db_param)
        )
        # Check if PRM in formula
        for tprm in db_tmo.tprms:  # type: TPRM
            if (
                tprm.val_type == "formula"
                and tprm.constraint
                and tprm.id != param.tprm_id
            ):
                try:
                    param_utils.update_prm_for_formula(
                        session=session, db_param_type=tprm, mos=[db_object]
                    )
                except ValueError as ex:
                    error_list.append({"error": ex.args})
                    continue
        # Check if PRM in point_a, point_b
        stmt = select(MO).where(MO.point_a_id == db_object.id)
        list_mo_for_update: list[MO] = session.execute(stmt).scalars().all()
        for mo in list_mo_for_update:
            utils.update_geometry(
                object_instance=mo, point_a=db_object, point_b=mo.point_b
            )
            flag_modified(mo, "geometry")
        stmt = select(MO).where(MO.point_b_id == db_object.id)
        list_mo_for_update: list[MO] = session.execute(stmt).scalars().all()
        for mo in list_mo_for_update:
            utils.update_geometry(
                object_instance=mo, point_a=mo.point_a, point_b=db_object
            )
            flag_modified(mo, "geometry")
    return db_param_list, error_list
