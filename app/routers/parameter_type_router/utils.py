import pickle
from typing import List

from fastapi import HTTPException
from sqlmodel import Session, select

from common.common_constant import (
    allowed_val_types,
    not_multiple_val_types,
    not_required_val_types,
    val_types_with_required_constraint,
)
from common.common_exceptions import ValidationError
from common.common_utils import ValueTypeValidator
from database import get_chunked_values_by_sqlalchemy_limit
from functions import functions_dicts
from functions.db_functions.db_delete import (
    params_deleting_by_changing_constraint,
)
from functions.functions_utils import utils
from functions.validation_functions import validation_function
from models import TMO, MO, TPRM, PRM
from routers.parameter_type_router.constants import RESERVED_NAMES_FOR_TPRMS
from routers.parameter_type_router.exceptions import ParameterTypeNotExists
from routers.parameter_type_router.schemas import (
    TPRMUpdateValtype,
    TPRMUpdate,
    TPRMUpdateWithTMO,
    TPRMCreateByTMO,
)


def compare_old_and_new_tprm(
    new_param_type_data: dict, old_param_type_data: dict
) -> bool:
    """Return False if old and new TPRMs are similar. IF they have different - response is True"""
    not_db_attribute_keys = ["force", "field_value"]

    if all(
        value is None
        for key, value in new_param_type_data.items()
        if key != "version"
    ) or all(
        old_param_type_data.get(key) == value
        for key, value in new_param_type_data.items()
        if key != "version" and key not in not_db_attribute_keys
    ):
        return False
    return True


def get_tprm_by_tmo_and_name(
    session: Session, tmo_id: int, tprm_name: str
) -> TPRM | None:
    """
    Returns TPRM instance by tmo id and name if it exists
    :param session: DB session instance
    :param tmo_id: id of TMO
    :param tprm_name: name of TPRM
    """
    query = select(TPRM).where(TPRM.name == tprm_name, TPRM.tmo_id == tmo_id)
    db_param_type = session.execute(query)
    return db_param_type.scalar()


def update_sequences_after_constraint_delete(
    session: Session, linked_tprms: list[TPRM]
):
    """
    If constraint TPRM was deleted, this function will rebuild the linked sequence and delete constraint
    :param session: DB session instance
    :param linked_tprms: list with TPRMs of 'sequence' val_type that had constraint to deleted TPRM
    """
    for linked_sequence in linked_tprms:
        rebuild_sequence_without_constraint(
            session=session, sequence_tprm_id=linked_sequence.id
        )
        delete_constraint_of_tprm(
            session=session, linked_tprm_id=linked_sequence.id
        )


def rebuild_sequence_without_constraint(
    session: Session, sequence_tprm_id: int
):
    """
    Rebuilds sequence for single TPRM if it doesn't have constraint
    :param session: DB session instance
    :param sequence_tprm_id: id of TPRM with 'sequence' val_type
    """
    query = (
        select(PRM)
        .where(PRM.tprm_id == sequence_tprm_id)
        .order_by(PRM.mo_id)
        .execution_options(yield_per=100)
    )
    linked_params = session.execute(query)
    sequence_value = 1
    for chunk in linked_params.scalars().partitions(100):
        for param in chunk:
            param.value = str(sequence_value)
            sequence_value += 1
            session.add(param)


def delete_constraint_of_tprm(session: Session, linked_tprm_id: int):
    """
    Deletes constraint of TPRM
    :param session: DB session instance
    :param linked_tprm_id: id of TPRM that linked by constraint
    """
    tprm_to_update: TPRM = session.exec(
        select(TPRM).where(TPRM.id == linked_tprm_id)
    ).first()
    tprm_to_update.constraint = None
    session.add(tprm_to_update)


def build_sequence(session: Session, tprm: TPRM):
    # get ids of MO
    mo_ids = session.execute(
        select(MO.id).where(MO.tmo_id == tprm.tmo_id).order_by(MO.id)
    )
    mo_ids = mo_ids.scalars().all()

    if not tprm.constraint:
        for value, mo_id in enumerate(mo_ids):
            parameter = PRM(tprm_id=tprm.id, mo_id=mo_id, value=str(value + 1))
            session.add(parameter)
            session.flush()
    else:
        # get unique values of constraint TPRM (sequence type)
        query = (
            select(PRM.value)
            .where(PRM.tprm_id == int(tprm.constraint))
            .distinct()
        )
        values = session.execute(query)
        values = dict.fromkeys(values.scalars().all(), 1)

        # get mo_id to sequence type mapper
        query = select(PRM.mo_id, PRM.value).where(
            PRM.tprm_id == int(tprm.constraint), PRM.mo_id.in_(mo_ids)
        )
        constraint_params = session.execute(query)
        constraint_params = {
            item[0]: item[1] for item in constraint_params.all()
        }

        # build sequence
        for mo_id in mo_ids:
            parameter = PRM(
                tprm_id=tprm.id,
                mo_id=mo_id,
                value=str(values[constraint_params[mo_id]]),
            )
            session.add(parameter)
            session.flush()
            values[constraint_params[mo_id]] += 1

    session.commit()


def get_list_trpms_by_tmo_and_val_type(
    session: Session, tmo_id: int, val_type: str
):
    query = select(TPRM).where(TPRM.tmo_id == tmo_id, TPRM.val_type == val_type)
    result = session.execute(query)
    result = result.scalars().all()
    return result


def validate_param_type_before_change_type(
    session: Session, db_param_type: TPRM, param_type: TPRMUpdateValtype
):
    if db_param_type.required and param_type.force:
        if param_type.field_value is None:
            raise HTTPException(
                status_code=409,
                detail="The parameter is required. You must specify a value to fill",
            )
        try:
            validation_task = ValueTypeValidator(
                session=session,
                parameter_type_instance=param_type,
                value_to_validate=param_type.field_value,
            )
            validation_task.validate()
        except ValidationError as e:
            raise HTTPException(status_code=e.status_code, detail=e.detail)
        validation_function.field_value_to_str_or_exception(
            field_value=param_type.field_value
        )


def validate_param_type_before_update(
    session: Session, db_param_type: TPRM, param_type: TPRMUpdate
):
    """Raises an HTTPException if an error occurs, otherwise return {'param_type_data': param_type_data,
    'field_value': field_value}"""
    tmo = session.execute(
        select(TMO).where(TMO.id == db_param_type.tmo_id)
    ).scalar_one()

    if param_type.name in RESERVED_NAMES_FOR_TPRMS:
        raise HTTPException(
            status_code=422,
            detail=f"There is name for TPRM, which can't be use, because names: {RESERVED_NAMES_FOR_TPRMS} "
            f"are reserved.",
        )

    param_type_data = (
        validation_function.param_type_data_validation_when_update(
            session=session, db_param_type=db_param_type, param_type=param_type
        )
    )
    if (
        param_type.constraint
        and param_type.constraint != db_param_type.constraint
    ):
        param_type = TPRMUpdateWithTMO(
            tmo_id=db_param_type.tmo_id, **param_type.dict()
        )
        functions_dicts.param_type_constraint_validation[
            db_param_type.val_type
        ](param_type, session)

        if not param_type.force:
            raise HTTPException(
                status_code=409,
                detail="Parameter values can be deleted as a result of this action."
                " You should pass force parameter (true) when changing constraint"
                " for parameter type.",
            )

        params_deleting_by_changing_constraint(
            session=session, db_param_type=db_param_type, param_type=param_type
        )
        db_param_type.constraint = param_type.constraint

    field_value = utils.field_value_if_update_to_required(
        db_param_type=db_param_type,
        param_type=param_type,
        param_type_data=param_type_data,
    )
    if not db_param_type.required and param_type.required:
        if db_param_type.val_type == "formula":
            return {"param_type_data": param_type_data, "field_value": ""}
        try:
            validation_task = ValueTypeValidator(
                session=session,
                parameter_type_instance=db_param_type,
                value_to_validate=field_value,
            )
            validation_task.validate()
        except ValidationError as e:
            raise HTTPException(
                status_code=e.status_code,
                detail=e.detail,
            )
        if db_param_type.multiple:
            field_value = pickle.dumps(field_value).hex()

        else:
            field_value = validation_function.field_value_to_str_or_exception(
                field_value=field_value
            )

    if (
        "required" in param_type.dict(exclude_unset=True)
        and not param_type.required
        and db_param_type.id in tmo.label
    ):
        raise HTTPException(
            status_code=409,
            detail="Unable change attribute 'required'. Param type is label for object type.",
        )
    if (
        "required" in param_type.dict(exclude_unset=True)
        and not param_type.required
        and db_param_type.id in tmo.primary
    ):
        raise HTTPException(
            status_code=409,
            detail="Unable change attribute 'required'. Param type is primary for object type.",
        )
    return {"param_type_data": param_type_data, "field_value": field_value}


def validate_param_type_before_create(
    session: Session, param_types: List[TPRMCreateByTMO], tmo_id: int
):
    """Returns errors list if there are validation errors, otherwise returns empty list"""
    error_list = []
    tprm_names = [tprm.name for tprm in param_types]
    # check if TPRMCreateByTMO names are unique
    stmt = select(TPRM).where(TPRM.tmo_id == tmo_id, TPRM.name.in_(tprm_names))
    tprms_from_db = session.exec(stmt).all()
    if tprms_from_db:
        tprms_from_db_names = [tprm.name for tprm in tprms_from_db]
        error_list.append(
            {"error": f"TPRM named: {tprms_from_db_names} already exist"}
        )

    for param_type in param_types:
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
            ](param_type, tmo_id, error_list, session):
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

            del param_type_data["field_value"]
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
                validation_task.validate()

            except ValidationError as e:
                error_list.append({"error": e.detail})
                continue

    return error_list


class ParameterTypeDBGetter:
    def __init__(self, session: Session):
        self._session = session

    def _get_parameter_type_instance_by_id(
        self, parameter_type_id: int
    ) -> TPRM | None:
        query = select(TPRM).where(TPRM.id == parameter_type_id)
        parameter_type_instance = self._session.execute(query).scalar()

        if parameter_type_instance:
            return parameter_type_instance

        raise ParameterTypeNotExists(
            status_code=422,
            detail=f"Parameter type with id {parameter_type_id} not found.",
        )

    def _get_parameters_type_by_ids(
        self, parameter_type_ids: set[int] | list[int]
    ) -> dict[int, TPRM]:
        parameter_type_instance_by_id = dict()

        for chunk in get_chunked_values_by_sqlalchemy_limit(parameter_type_ids):
            query = select(TPRM).where(TPRM.id.in_(chunk))
            temp_parameter_type_instance_by_id = {
                parameter_instance.id: parameter_instance
                for parameter_instance in self._session.execute(query)
                .scalars()
                .all()
            }
            parameter_type_instance_by_id.update(
                temp_parameter_type_instance_by_id
            )

        return parameter_type_instance_by_id

    def _get_parameter_type_instances_by_tmo_id(
        self, object_type_id: int, val_type: str
    ) -> list[TPRM]:
        query = select(TPRM).where(
            TPRM.tmo_id == object_type_id, TPRM.val_type == val_type
        )
        parameter_type_instances: list[TPRM] = (
            self._session.execute(query).scalars().all()
        )

        return parameter_type_instances
