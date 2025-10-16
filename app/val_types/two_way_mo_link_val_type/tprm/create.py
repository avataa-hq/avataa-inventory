import dataclasses
from collections import defaultdict
from typing import TypeAlias

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import TPRM, TMO, TPRMBase
from routers.parameter_type_router.constants import RESERVED_NAMES_FOR_TPRMS
from routers.parameter_type_router.schemas import TPRMCreate, TPRMUpdate
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)
from val_types.constants import two_way_mo_link_val_type_name, ErrorHandlingType

TmoId: TypeAlias = int
TprmId: TypeAlias = int


@dataclasses.dataclass(slots=True)
class TprmErrorMsg:
    msg: str
    tprm: TPRMBase | TPRMUpdate


@dataclasses.dataclass(frozen=True, slots=True)
class RemoveTprmFrozenData:
    name: str
    tmo_id: int


def check_new_two_way_tprms(
    session: Session, new_tprms: list[TPRMCreate]
) -> tuple[list[TprmErrorMsg], list[TPRMCreate]]:
    errors: list[TprmErrorMsg] = []
    cleared_tprms: list[TPRMCreate] = []

    constraint_tmo_ids: dict[TmoId, list[TPRMCreate]] = defaultdict(list)
    tmo_ids: dict[TmoId, list[TPRMCreate]] = defaultdict(list)
    straight_and_backward_tmo_ids: dict[TmoId, list[TPRMCreate]] = defaultdict(
        list
    )
    for tprm in new_tprms:
        tprm_errors: list[TprmErrorMsg] = []
        if tprm.val_type != two_way_mo_link_val_type_name:
            msg = "TPRM type is not two-way link."
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        if tprm.multiple:
            msg = "TPRM cannot be multiple."
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        if tprm.required:
            msg = "TPRM cannot be required."
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        if tprm.field_value:
            msg = "The Field Value parameter is not applicable to this value type."
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        if tprm.prm_link_filter:
            msg = "The PRM link filter parameter is not applicable to this value type."
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)

        tmo_id = None
        if not tprm.constraint:
            msg = "TPRM constraint cannot be empty."
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        elif not tprm.constraint.isdigit():
            msg = "TPRM constraint must contain one tmo id"
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        else:
            try:
                tmo_id = int(tprm.constraint)
            except ValueError:
                msg = "Failed to convert a constraint to a number."
                error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
                tprm_errors.append(error_msg)
            else:
                if tmo_id == tprm.tmo_id:
                    msg = "Constraint cannot refer to an object of the same object type"
                    error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
                    tprm_errors.append(error_msg)
                else:
                    constraint_tmo_ids[tmo_id].append(tprm)

        tmo_ids[tprm.tmo_id].append(tprm)
        straight_and_backward_tmo_ids[tprm.tmo_id].append(tprm)
        if tmo_id:
            straight_and_backward_tmo_ids[tmo_id].append(tprm)

        if tprm.name in RESERVED_NAMES_FOR_TPRMS:
            msg = "There is name for TPRM, which can't be use, because names: {0} are reserved.".format(
                RESERVED_NAMES_FOR_TPRMS
            )
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)
        check_name_is_unique_stmt = select(TPRM).where(
            TPRM.tmo_id.in_((tprm.tmo_id, tmo_id)), TPRM.name == tprm.name
        )
        if session.execute(check_name_is_unique_stmt).scalar_one_or_none():
            msg = "The parameter type name must be unique within a single object type"
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            tprm_errors.append(error_msg)

        if tprm_errors:
            errors.extend(tprm_errors)
        else:
            cleared_tprms.append(tprm)

    remove_from_cleared = set()
    if tmo_ids:
        check_existing_tmo_ids_stmt = select(TMO.id).where(
            TMO.id.in_(tmo_ids.keys())
        )
        existing_tmo_ids = set(
            session.execute(check_existing_tmo_ids_stmt).scalars()
        )
        not_existing_tmo_ids = set(tmo_ids.keys()).difference(existing_tmo_ids)
        for not_existing_tmo_id in not_existing_tmo_ids:
            for tprm in tmo_ids[not_existing_tmo_id]:
                msg = "TPRM mo_id refers to a non-existing object type id"
                error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
                errors.append(error_msg)
                remove_obj = RemoveTprmFrozenData(
                    name=tprm.name, tmo_id=tprm.tmo_id
                )
                remove_from_cleared.add(remove_obj)

    for tprms_group in straight_and_backward_tmo_ids.values():
        tprms_by_names = defaultdict(list)
        for tprm in tprms_group:
            tprms_by_names[tprm.name].append(tprm)
        for tprms_name, tprms in tprms_by_names.items():
            if len(tprms) == 1:
                continue
            tprm_errors = []
            for tprm in tprms:
                msg = "The parameter type name must be unique within a single object type"
                error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
                tprm_errors.append(error_msg)
                remove_obj = RemoveTprmFrozenData(
                    name=tprm.name, tmo_id=tprm.tmo_id
                )
                remove_from_cleared.add(remove_obj)
            errors.extend(tprm_errors)

    if constraint_tmo_ids:
        check_existing_tmo_ids_stmt = select(TMO.id).where(
            TMO.id.in_(constraint_tmo_ids.keys())
        )  # type: ignore # pylint: disable=E1137
        existing_tmo_ids = set(
            session.execute(check_existing_tmo_ids_stmt).scalars()
        )
        not_existing_tmo_ids = set(constraint_tmo_ids).difference(
            existing_tmo_ids
        )
        for not_existing_tmo_id in not_existing_tmo_ids:
            for tprm in constraint_tmo_ids[not_existing_tmo_id]:
                msg = "TPRM constraint refers to a non-existing object type id"
                error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
                errors.append(error_msg)
                remove_obj = RemoveTprmFrozenData(
                    name=tprm.name, tmo_id=tprm.tmo_id
                )
                remove_from_cleared.add(remove_obj)

    if remove_from_cleared:
        cleared_tprms = [
            i
            for i in cleared_tprms
            if RemoveTprmFrozenData(name=i.name, tmo_id=i.tmo_id)
            not in remove_from_cleared
        ]
    return errors, cleared_tprms


def create_another_tprm_id_and_update_existed(
    session: Session, tprms: list[TPRM]
) -> list[TPRM]:
    tprms_by_existing_tprm_id: dict[TprmId, TPRM] = {}
    another_tprms_by_existing_tprm_id: dict[TprmId, TPRM] = {}
    for tprm in tprms:
        tprms_by_existing_tprm_id[tprm.id] = tprm
        another_tmo_id = int(tprm.constraint)
        another_constraint = str(tprm.tmo_id)
        update = {
            "tmo_id": another_tmo_id,
            "constraint": another_constraint,
            "backward_link": tprm.id,
            "id": None,
        }
        another_tprm = TPRM.from_orm(tprm, update=update)
        another_tprms_by_existing_tprm_id[tprm.id] = another_tprm
    session.add_all(another_tprms_by_existing_tprm_id.values())
    session.flush(another_tprms_by_existing_tprm_id.values())
    for tprm_id, another_tprm in another_tprms_by_existing_tprm_id.items():
        session.refresh(another_tprm)
        tprm = tprms_by_existing_tprm_id[tprm_id]
        tprm.backward_link = another_tprm.id

    session.flush(tprms_by_existing_tprm_id.values())
    for tprm in tprms_by_existing_tprm_id.values():
        session.refresh(tprm)

    results = [
        *tprms_by_existing_tprm_id.values(),
        *another_tprms_by_existing_tprm_id.values(),
    ]
    return results


def create_cleared_two_way_mo_link_tprms(
    session: Session, new_tprms: list[TPRMCreate], user_name: str | None = None
) -> tuple[list[TprmErrorMsg], list[TPRM]]:
    errors: list[TprmErrorMsg] = []
    tprms: list[TPRM] = []
    if not new_tprms:
        return errors, tprms
    user_name = (
        get_username_from_session(session=session)
        if user_name is None
        else user_name
    )
    update = {"modified_by": user_name, "created_by": user_name}
    converted_tprms: list[TPRM] = [
        TPRM.from_orm(i, update=update) for i in new_tprms
    ]
    session.add_all(converted_tprms)
    try:
        session.flush()
        for tprm in converted_tprms:
            session.refresh(tprm)
        tprms = create_another_tprm_id_and_update_existed(
            session=session, tprms=converted_tprms
        )
    except IntegrityError:
        session.rollback()
        msg = "Failed to create tprm."
        for tprm in new_tprms:
            error_msg = TprmErrorMsg(msg=msg, tprm=tprm)
            errors.append(error_msg)
        return errors, tprms
    else:
        return errors, tprms


def format_error(error: TprmErrorMsg):
    return f"{error.msg}. TMO ID: {error.tprm.tmo_id}, TPRM name: {error.tprm.name}"


def create_two_way_mo_link_tprms(
    session: Session,
    new_tprms: list[TPRMCreate],
    in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
    autocommit: bool = True,
) -> tuple[list[str], list[TPRMCreate | TPRM]]:
    errors, cleared_new_tprms = check_new_two_way_tprms(
        session=session, new_tprms=new_tprms
    )
    if errors and in_case_of_error == ErrorHandlingType.RAISE_ERROR:
        raise ValueError("\n".join(map(format_error, errors)))

    formatted_errors = [format_error(error=err) for err in errors]
    if in_case_of_error == ErrorHandlingType.ONLY_CHECKING:
        return formatted_errors, cleared_new_tprms
    additional_errors, created_tprms = create_cleared_two_way_mo_link_tprms(
        new_tprms=cleared_new_tprms, session=session
    )
    if created_tprms:
        if autocommit:
            session.commit()
        for tprm in created_tprms:
            session.refresh(tprm)
    additional_errors = [format_error(error=err) for err in additional_errors]
    formatted_errors.extend(additional_errors)
    return formatted_errors, created_tprms
