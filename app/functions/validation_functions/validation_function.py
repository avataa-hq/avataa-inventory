import pickle
import re
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlmodel import select, Session

from common.common_constant import (
    allowed_val_types,
    not_multiple_val_types,
    not_required_val_types,
    val_types_cannot_be_changed_to,
)
from database import get_chunked_values_by_sqlalchemy_limit
from functions import functions_dicts as func
from functions.db_functions import db_read
from functions.functions_utils import utils
from functions.validation_functions import validation_utils
from models import TPRM, PRM, MO
from routers.object_router.schemas import MOUpdate, MOCreateWithParams
from routers.parameter_router.schemas import PRMCreateByMO, PRMReadMultiple
from routers.parameter_type_router.schemas import (
    TPRMCreate,
    TPRMUpdateValtype,
    TPRMUpdate,
)
from routers.parameter_type_router import utils as parameter_type_utils
from val_types.constants import enum_val_type_name


def check_if_all_required_params_passed(
    session: Session, object: MOCreateWithParams
) -> None:
    required_ids = set()
    received_ids = set()
    session.info["disable_security"] = True
    param_types = session.exec(
        select(TPRM).where(TPRM.tmo_id == object.tmo_id, TPRM.required == True)  # noqa
    ).all()
    for param_type in param_types:
        # If we got formula val type we don't check TPRM
        # If we're unable to create MO, raise Error else set empty string as PRM value
        if param_type.val_type != "formula":
            required_ids.add(param_type.id)
    for param in object.params:
        if param.tprm_id in required_ids:
            received_ids.add(param.tprm_id)
    if required_ids > received_ids:
        unreceived_ids = list(required_ids.difference(received_ids))
        raise HTTPException(
            status_code=400,
            detail=f"Not all required parameters passed. Add parameters with tprm_id: {unreceived_ids}",
        )


def single_value_to_str_or_exception(value: Any) -> str:
    if isinstance(value, (str, bool, float, int)):
        value = str(value)
    else:
        raise HTTPException(status_code=422, detail="Incorrect param value.")
    return value


def check_if_multiple_param_value_is_not_empty_list(
    multiple_value: Any,
) -> None:
    if isinstance(multiple_value, list):
        if len(multiple_value) == 0:
            raise HTTPException(
                status_code=422,
                detail="Invalid multiple param value: empty list.",
            )
    else:
        raise HTTPException(
            status_code=422,
            detail="Invalid multiple param value. Please, pass the list.",
        )


def check_if_single_values_in_list_are_valid(multiple_value):
    for single_val in multiple_value:
        if single_val is None or isinstance(single_val, (list, dict)):
            raise HTTPException(
                status_code=422, detail=f"Invalid value in list: {single_val}"
            )


def val_type_validation_when_create_param_type(param_type: TPRMCreate) -> None:
    if param_type.val_type.lower() in allowed_val_types:
        param_type.val_type = param_type.val_type.lower()
    else:
        raise HTTPException(
            status_code=422,
            detail=f"Incorrect valtype. Allowed valtypes: {allowed_val_types}.",
        )
    if param_type.val_type in not_multiple_val_types and param_type.multiple:
        raise HTTPException(
            status_code=409,
            detail=f"Not allowed to create multiple parameter for {param_type.val_type} val_type.",
        )
    if param_type.val_type in not_required_val_types and param_type.required:
        raise HTTPException(
            status_code=409,
            detail=f"Not allowed to create required parameter for {param_type.val_type} val_type.",
        )


def field_value_is_not_none_for_required_validation(
    param_type: TPRMCreate,
) -> Any:
    param_type_data = param_type.dict(exclude_unset=True)
    if "field_value" in param_type_data:
        if param_type_data["field_value"] is None:
            raise HTTPException(
                status_code=400,
                detail="Please, pass the 'field_value' when creating required parameter type.",
            )
        field_value = param_type_data["field_value"]
    elif param_type.val_type == "formula":
        return None
    else:
        raise HTTPException(
            status_code=400,
            detail="Please, pass the 'field_value' when creating required parameter type.",
        )
    return field_value


def check_if_multiple_field_value_is_not_empty_list(field_value: Any) -> None:
    if isinstance(field_value, list):
        if len(field_value) == 0:
            raise HTTPException(
                status_code=422,
                detail="Invalid multiple field_value: empty list.",
            )
    else:
        raise HTTPException(
            status_code=422,
            detail="Invalid multiple field_value. Please, pass the list.",
        )


def field_value_to_str_or_exception(field_value: Any) -> str:
    if isinstance(field_value, (str, bool, int, float)):
        field_value = str(field_value)
    else:
        raise HTTPException(status_code=422, detail="Invalid field_value")
    return field_value


def param_type_data_validation_when_update(
    session: Session, db_param_type: TPRM, param_type: TPRMUpdate
) -> dict:
    if db_param_type.version != param_type.version:
        raise HTTPException(
            status_code=409,
            detail=f"Actual version of TPRM: {db_param_type.version}.",
        )

    param_type_data = param_type.dict(exclude_unset=True)

    if not parameter_type_utils.compare_old_and_new_tprm(
        new_param_type_data=param_type.dict(exclude_unset=True),
        old_param_type_data=db_param_type.dict(),
    ):
        if "force" in param_type_data:
            del param_type_data["force"]
        return param_type_data

    db_object_type = db_read.get_db_object_type_or_exception(
        session=session, object_type_id=db_param_type.tmo_id
    )

    if (
        "required" in param_type_data
        and not param_type_data["required"]
        and db_param_type.id in db_object_type.primary
    ):
        raise HTTPException(
            status_code=422, detail="Primary parameter type should be required."
        )

    if "name" in param_type_data:
        not_unique = session.exec(
            select(TPRM).where(
                TPRM.name == param_type.name,
                TPRM.tmo_id == db_param_type.tmo_id,
                TPRM.id != db_param_type.id,
            )
        ).first()
        if not_unique:
            raise HTTPException(
                status_code=422,
                detail=f"Param type with name '{not_unique.name}' "
                f"and tmo_id {not_unique.tmo_id} already exists!",
            )

    if "required" in param_type_data and param_type_data["required"] is None:
        raise HTTPException(
            status_code=422, detail="'required' field can't be null."
        )

    if db_param_type.val_type in ["prm_link", "sequence"]:
        if param_type.required:
            raise HTTPException(
                status_code=409,
                detail=f"Not allowed to change {db_param_type.val_type} parameter type to required.",
            )

    if param_type.constraint:
        if (db_param_type.required and param_type.required) or (
            db_param_type.required and param_type.required is None
        ):
            raise HTTPException(
                status_code=409,
                detail="Not allowed to change constraint for required parameter.",
            )
    param_type_data["version"] += 1

    if "force" in param_type_data:
        del param_type_data["force"]
    return param_type_data


def param_type_force_parameter_validation(param_type) -> None:
    if not param_type.force:
        raise HTTPException(
            status_code=409,
            detail="Parameter values can be deleted as a result of this action."
            " You should pass force parameter (true) when changing constraint"
            " for parameter type.",
        )


def val_type_validation_when_update(param_type: TPRMUpdateValtype) -> None:
    if param_type.val_type.lower() in allowed_val_types:
        param_type.val_type = param_type.val_type.lower()
        if param_type.val_type in val_types_cannot_be_changed_to:
            raise HTTPException(
                status_code=422,
                detail=f"Not allowed to change val_type to {param_type.val_type}.",
            )
    else:
        raise HTTPException(
            status_code=422,
            detail=f"Incorrect valtype. Allowed valtypes: {allowed_val_types}.",
        )


def params_validation_and_convertation_when_change_val_type(
    session: Session, param_type: TPRMUpdateValtype, db_param_type: TPRM
) -> None:
    for param in db_param_type.prms:
        if param_type.val_type == "str":
            if db_param_type.multiple:
                multiple_value = utils.decode_multiple_value(value=param.value)
                for i in range(len(multiple_value)):
                    multiple_value[i] = str(multiple_value[i])

                validation_utils.update_parameter_multiple_value_for_str(
                    session=session,
                    multiple_value=multiple_value,
                    prm_id=param.id,
                )
        if db_param_type.val_type == enum_val_type_name:
            raise HTTPException(
                status_code=422,
                detail="Not allowed to change val_type from enum_val_type",
            )

        if param_type.val_type == "bool":
            if db_param_type.multiple:
                multiple_value = utils.decode_multiple_value(value=param.value)
                for value in multiple_value:
                    if not isinstance(value, bool):
                        validation_utils.delete_parameter_by_prm_link_multiple_value(
                            session=session, prm_id=param.id, param=param
                        )
                        session.delete(param)
                        break
            else:
                if param.value.lower() == "true":
                    validation_utils.update_parameter_bool_value(
                        session=session, status="True", param=param
                    )
                elif param.value.lower() == "false":
                    validation_utils.update_parameter_bool_value(
                        session=session, status="False", param=param
                    )
                else:
                    if not db_param_type.required:
                        validation_utils.delete_parameter_by_prm_link_value(
                            session=session, param=param
                        )
                        session.delete(param)
                    else:
                        session.info["disable_security"] = True
                        db_param = db_read.get_db_param_or_exception(
                            session=session, prm_id=param.id
                        )
                        db_param.value = db_param_type.field_value
                        session.add(db_param)
        if param_type.val_type == "float":
            if db_param_type.multiple:
                multiple_value = utils.decode_multiple_value(value=param.value)
                all_single_value_correct = True
                for value in multiple_value:
                    if not isinstance(value, (int, float)) or isinstance(
                        value, bool
                    ):
                        validation_utils.delete_parameter_by_prm_link_multiple_value(
                            session=session, prm_id=param.id, param=param
                        )
                        session.delete(param)
                        all_single_value_correct = False
                        break
                if all_single_value_correct:
                    for i in range(len(multiple_value)):
                        multiple_value[i] = float(multiple_value[i])
                    session.info["disable_security"] = True
                    db_param = db_read.get_db_param_or_exception(
                        session=session, prm_id=param.id
                    )
                    new_value = pickle.dumps(multiple_value).hex()
                    db_param.value = new_value
                    session.add(db_param)
            else:
                try:
                    float(param.value)
                except ValueError:
                    if not db_param_type.required:
                        validation_utils.delete_parameter_by_prm_link_value(
                            session=session, param=param
                        )
                        session.delete(param)
                    else:
                        session.info["disable_security"] = True
                        db_param = db_read.get_db_param_or_exception(
                            session=session, prm_id=param.id
                        )
                        db_param.value = db_param_type.field_value
                        session.add(db_param)
        if param_type.val_type == "int":
            if db_param_type.multiple:
                multiple_value = utils.decode_multiple_value(value=param.value)
                all_single_value_correct = True
                for value in multiple_value:
                    if not isinstance(value, (int, float)) or isinstance(
                        value, bool
                    ):
                        validation_utils.delete_parameter_by_prm_link_multiple_value(
                            session=session, prm_id=param.id, param=param
                        )
                        session.delete(param)
                        all_single_value_correct = False
                        break
                if all_single_value_correct:
                    for i in range(len(multiple_value)):
                        multiple_value[i] = int(multiple_value[i])
                    validation_utils.update_parameter_multiple_value_for_str(
                        session=session,
                        multiple_value=multiple_value,
                        prm_id=param.id,
                    )
            else:
                try:
                    int(param.value)
                except ValueError:
                    if not db_param_type.required:
                        validation_utils.delete_parameter_by_prm_link_value(
                            session=session, param=param
                        )
                        session.delete(param)
                    else:
                        session.info["disable_security"] = True
                        db_param = db_read.get_db_param_or_exception(
                            session=session, prm_id=param.id
                        )
                        db_param.value = db_param_type.field_value

                        session.add(db_param)
        if param_type.val_type == "datetime":
            if db_param_type.multiple:
                multiple_value = utils.decode_multiple_value(value=param.value)
                for value in multiple_value:
                    try:
                        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                    except:  # noqa
                        validation_utils.delete_parameter_by_prm_link_multiple_value(
                            session=session, prm_id=param.id, param=param
                        )
                        session.delete(param)
                        break
            else:
                try:
                    datetime.strptime(param.value, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    if not db_param_type.required:
                        validation_utils.delete_parameter_by_prm_link_value(
                            session=session, param=param
                        )
                        session.delete(param)
                    else:
                        session.info["disable_security"] = True
                        db_param = db_read.get_db_param_or_exception(
                            session=session, prm_id=param.id
                        )
                        db_param.value = db_param_type.field_value
                        session.add(db_param)
        if param_type.val_type == "date":
            if db_param_type.multiple:
                multiple_value = utils.decode_multiple_value(value=param.value)
                for value in multiple_value:
                    try:
                        datetime.strptime(value, "%Y-%m-%d")
                    except:  # noqa
                        validation_utils.delete_parameter_by_prm_link_multiple_value(
                            session=session, prm_id=param.id, param=param
                        )
                        session.delete(param)
                        break
            else:
                try:
                    datetime.strptime(param.value, "%Y-%m-%d")
                except ValueError:
                    if not db_param_type.required:
                        validation_utils.delete_parameter_by_prm_link_value(
                            session=session, param=param
                        )
                        session.delete(param)
                    else:
                        session.info["disable_security"] = True
                        db_param = db_read.get_db_param_or_exception(
                            session=session, prm_id=param.id
                        )
                        db_param.value = db_param_type.field_value

                        session.add(db_param)


def force_field_validation_when_update_val_type(param_type, db_param_type):
    param_type_data = param_type.dict(exclude_unset=True)
    if "force" not in param_type_data:
        force = False
    else:
        force = param_type_data["force"]
    if len(db_param_type.prms) > 0 and not force:
        raise HTTPException(
            status_code=409,
            detail="Parameter values can be deleted as a result of this action."
            " You should pass force parameter (true) when changing val_type for parameter type.",
        )


def validate_param_by_prm_link_filter(
    session: Session,
    db_param_type: TPRM,
    param: PRMCreateByMO,
    db_object: MOCreateWithParams,
):
    regex = re.compile(r"(\d+):(\d+)")
    internal_tprm_id, external_tprm_id = regex.findall(
        db_param_type.prm_link_filter
    )[0]
    internal_tprm_id = int(internal_tprm_id)
    external_tprm_id = int(external_tprm_id)

    internal_parameter_link = None
    for prm in db_object.params:
        if prm.tprm_id == internal_tprm_id:
            internal_parameter_link = prm
            break

    if internal_parameter_link:
        possible_prm_ids = (
            validation_utils.get_possible_prm_ids_for_internal_link(
                session=session,
                external_tprm_id=external_tprm_id,
                internal_parameter_link=internal_parameter_link,
                db_param_type=db_param_type,
            )
        )
    else:
        possible_prm_ids = (
            validation_utils.get_possible_prm_ids_for_external_link(
                session=session,
                external_tprm_id=external_tprm_id,
                db_param_type=db_param_type,
            )
        )

    linked_parameter = session.execute(
        select(PRM).where(PRM.id == param.value)
    ).scalar()
    if linked_parameter.id not in possible_prm_ids:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid  value ({linked_parameter.id}) in prm_link parameter."
            f" Available link ids: {possible_prm_ids}.",
        )


def multiple_validate_param_by_prm_link_filter(
    session: Session,
    db_param_type: TPRM,
    param: PRMCreateByMO,
    db_object: MOCreateWithParams,
):
    regex = re.compile(r"(\d+):(\d+)")
    internal_tprm_id, external_tprm_id = regex.findall(
        db_param_type.prm_link_filter
    )[0]
    internal_tprm_id = int(internal_tprm_id)
    external_tprm_id = int(external_tprm_id)

    prm_link_ids: list[int] = []
    for chunk in get_chunked_values_by_sqlalchemy_limit(param.value):
        prm_link_ids.extend(
            session.execute(select(PRM.id).where(PRM.id.in_(chunk)))
            .scalars()
            .all()
        )

    internal_parameter_link = None
    for prm in db_object.params:
        if prm.tprm_id == internal_tprm_id:
            internal_parameter_link = prm
            break

    if internal_parameter_link:
        possible_prm_ids = (
            validation_utils.get_possible_prm_ids_for_internal_link(
                session=session,
                external_tprm_id=external_tprm_id,
                internal_parameter_link=internal_parameter_link,
                db_param_type=db_param_type,
            )
        )
    else:
        possible_prm_ids = (
            validation_utils.get_possible_prm_ids_for_external_link(
                session=session,
                external_tprm_id=external_tprm_id,
                db_param_type=db_param_type,
            )
        )

    for linked_parameter_id in prm_link_ids:
        if linked_parameter_id not in possible_prm_ids:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid single value ({linked_parameter_id}) in multiple prm_link parameter."
                f" Available link ids: {possible_prm_ids}.",
            )


def get_param_to_read_based_on_multiple(db_param: PRM):
    if db_param.tprm.multiple:
        multiple_value = utils.decode_multiple_value(db_param.value)
        param_to_read = PRMReadMultiple(
            id=db_param.id,
            tprm_id=db_param.tprm_id,
            mo_id=db_param.mo_id,
            value=multiple_value,
            version=db_param.version,
        )
    else:
        param_to_read = func.db_param_convert_by_val_type[
            db_param.tprm.val_type
        ](
            db_param.id,
            db_param.tprm_id,
            db_param.mo_id,
            db_param.value,
            db_param.version,
        )

    return param_to_read


def object_data_validation_when_update(
    db_object: MO, upd_object: MOUpdate
) -> bool:
    """Validation MO object before update. Check correct version and difference
    between db_object and new data"""
    object_data = upd_object.dict(exclude_unset=True)
    if db_object.version != upd_object.version:
        raise HTTPException(
            status_code=409,
            detail=f"Actual version of MO: {db_object.version}.",
        )
    db_data_object = db_object.dict()
    exclude = ["version"]
    return not all(
        db_data_object.get(k) == v
        for k, v in object_data.items()
        if k not in exclude
    )
