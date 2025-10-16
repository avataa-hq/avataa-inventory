import dataclasses
import traceback
from collections import defaultdict
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import TPRM, TMO, PRM
from routers.parameter_type_router.constants import RESERVED_NAMES_FOR_TPRMS
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)

from routers.parameter_type_router.schemas import TPRMUpdate
from val_types.constants import ErrorHandlingType
from val_types.two_way_mo_link_val_type.tprm.create import (
    TprmErrorMsg,
    TmoId,
    TprmId,
    create_another_tprm_id_and_update_existed,
)


@dataclasses.dataclass(frozen=True, slots=True)
class RemoveTprmFrozenData:
    name: str
    tprm_id: int


class TprmUpdateWithId(TPRMUpdate):
    id: int


def check_update_two_way_tprms(
    session: Session, update_tprms: dict[TprmId, TPRMUpdate]
) -> tuple[list[TprmErrorMsg], list[TPRM]]:
    errors: list[TprmErrorMsg] = []

    existing_tprms_stmt = select(TPRM).where(TPRM.id.in_(update_tprms.keys()))
    existing_tprms_dict = {
        i.id: i for i in session.execute(existing_tprms_stmt).scalars()
    }

    remove_from_cleared = set()
    cleared_tprms: list[TPRM] = []
    constraint_tmo_ids: dict[TmoId, list[tuple[TprmId, TPRMUpdate]]] = (
        defaultdict(list)
    )
    tprms_by_backward_id: dict[TmoId, dict[TprmId, TprmUpdateWithId]] = (
        defaultdict(dict)
    )
    for tprm_id, update_tprm in update_tprms.items():
        tprm_errors = []
        update_tprm_with_id = TprmUpdateWithId.from_orm(
            update_tprm, update={"id": tprm_id}
        )
        update_attrs = update_tprm.dict(exclude_unset=True)
        existing_tprm: TPRM | None = existing_tprms_dict.get(tprm_id, None)
        if not existing_tprm:
            msg = "TPRM ID is not exist"
            error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
            tprm_errors.append(error_msg)
        else:
            tprms_by_backward_id[existing_tprm.backward_link][tprm_id] = (
                update_tprm_with_id
            )
            if existing_tprm.id in tprms_by_backward_id:
                inner_dict = tprms_by_backward_id[existing_tprm.id]
                if existing_tprm.backward_link in inner_dict:
                    msg = "Attempting to change both related parameter types"
                    error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                    tprm_errors.append(error_msg)
                    another_tprm_update_with_id = inner_dict[
                        existing_tprm.backward_link
                    ]
                    error_msg = TprmErrorMsg(
                        msg=msg, tprm=another_tprm_update_with_id
                    )
                    tprm_errors.append(error_msg)
                    remove_obj = another_tprm_update_with_id.id
                    remove_from_cleared.add(remove_obj)
            if update_tprm.version != existing_tprm.version:
                msg = "Version mismatch"
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)
            else:
                for update_attr in update_attrs:
                    if not hasattr(existing_tprm, update_attr):
                        continue
                    old_attr = getattr(existing_tprm, update_attr)
                    new_attr = getattr(update_tprm, update_attr)
                    if old_attr != new_attr:
                        break
                else:
                    msg = "Nothing changed"
                    error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                    tprm_errors.append(error_msg)
        if "required" in update_attrs and update_tprm.required:
            msg = "TPRM cannot be multiple."
            error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
            tprm_errors.append(error_msg)
        if "prm_link_filter" in update_attrs and update_tprm.prm_link_filter:
            msg = "The PRM link filter parameter is not applicable to this value type."
            error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
            tprm_errors.append(error_msg)
        if "field_value" in update_attrs and update_tprm.field_value:
            msg = "The field_value parameter is not applicable to this value type."
            error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
            tprm_errors.append(error_msg)
        tmo_id = None
        if "constraint" in update_attrs:
            if not update_tprm.constraint:
                msg = "TPRM constraint cannot be empty."
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)
            elif not update_tprm.constraint.isdigit():
                msg = "TPRM constraint must contain one tmo id"
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)
            else:
                try:
                    tmo_id = int(update_tprm.constraint)
                except ValueError:
                    msg = "Failed to convert a constraint to a number."
                    error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                    tprm_errors.append(error_msg)
                else:
                    constraint_tmo_ids[tmo_id].append((tprm_id, update_tprm))
            if existing_tprm and tmo_id == existing_tprm.tmo_id:
                msg = "Constraint cannot refer to an object of the same object type"
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)
            elif (
                not update_tprm.force
                and tmo_id
                and not tprm_errors
                and "constraint" in update_attrs
                and update_tprm.constraint != existing_tprm.constraint
            ):
                msg = (
                    "Parameter values can be deleted as a result of this action. "
                    "You should pass force parameter (true) when changing constraint for parameter type."
                )
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)

        if "name" in update_attrs:
            if not update_tprm.name:
                msg = "Name cannot be empty."
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)
            elif update_tprm.name.lower() in RESERVED_NAMES_FOR_TPRMS:
                msg = "There is name for TPRM, which can't be use, because names: {0} are reserved.".format(
                    RESERVED_NAMES_FOR_TPRMS
                )
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                tprm_errors.append(error_msg)

            if existing_tprm:
                check_name_is_unique_stmt = select(TPRM).where(
                    TPRM.tmo_id.in_((existing_tprm.tmo_id, tmo_id)),
                    TPRM.name == update_tprm.name,
                    TPRM.id.not_in(
                        (existing_tprm.id, int(existing_tprm.backward_link))
                    ),
                )
                if session.execute(
                    check_name_is_unique_stmt
                ).scalar_one_or_none():
                    msg = "The parameter type name must be unique within a single object type"
                    error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                    tprm_errors.append(error_msg)

        if tprm_errors:
            errors.extend(tprm_errors)
        else:
            # tprm = TPRM.from_orm(existing_tprm, update=update_attrs)
            tprm = existing_tprm
            cleared_tprms.append(tprm)

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
            for tprm_id, update_tprm in constraint_tmo_ids[not_existing_tmo_id]:
                update_tprm_with_id = TprmUpdateWithId.from_orm(
                    update_tprm, update={"id": tprm_id}
                )
                msg = "TPRM constraint refers to a non-existing object type id"
                error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
                errors.append(error_msg)
                remove_obj = tprm_id
                remove_from_cleared.add(remove_obj)

    if remove_from_cleared:
        cleared_tprms = [
            i for i in cleared_tprms if i.id not in remove_from_cleared
        ]
    return errors, cleared_tprms


def update_cleared_two_way_mo_link_tprms(
    tprms: list[TPRM],
    update_tprms: dict[TprmId, TPRMUpdate],
    session: Session,
    user_name: str | None = None,
) -> tuple[list[TprmErrorMsg], list[TPRM]]:
    now = datetime.utcnow()
    errors: list[TprmErrorMsg] = []
    updated_tprms: list[TPRM] = []
    if not tprms:
        return errors, tprms
    user_name = (
        get_username_from_session(session=session)
        if user_name is None
        else user_name
    )

    backward_tprm_ids = [i.id for i in tprms]
    backward_tprms_stmt = select(TPRM).where(
        TPRM.backward_link.in_(backward_tprm_ids)
    )
    backward_tprms_dict: dict[TprmId | None, TPRM] = {
        i.id: i for i in session.execute(backward_tprms_stmt).scalars()
    }

    tprms_to_delete: list[TPRM] = []
    tprms_to_create: list[TPRM] = []
    tprms_to_update: list[TPRM] = []
    for tprm in tprms:
        old_constraint = tprm.constraint
        update_tprm = update_tprms[tprm.id]
        update_tprm.version += 1
        tprm.modified_by = user_name
        tprm.modification_date = now
        for attr, value in update_tprm.dict(exclude_unset=True).items():
            if hasattr(tprm, attr):
                setattr(tprm, attr, value)
        tprms_to_update.append(tprm)

        if update_tprm.constraint != old_constraint:
            backward_tprm: TPRM | None = backward_tprms_dict.get(
                tprm.backward_link, None
            )
            if backward_tprm is not None:
                tprms_to_delete.append(backward_tprm)
                tprm.backward_link = None
            tprms_to_create.append(tprm)
    try:
        updated_tprm_ids = set()
        if tprms_to_update:
            tprms_by_id = {i.id: i for i in tprms_to_update}
            stmt_update_tprms = select(TPRM).where(
                TPRM.id.in_(tprms_by_id.keys())
            )
            for tprm_to_update in session.execute(stmt_update_tprms).scalars():
                updated_tprm_ids.add(tprm_to_update.id)
                update = tprms_by_id[tprm_to_update.id]
                for attr, value in update.dict(exclude_unset=True).items():
                    if hasattr(tprm_to_update, attr):
                        setattr(tprm_to_update, attr, value)
                updated_tprms.append(tprm_to_update)
        if tprms_to_delete:
            for tprm_to_delete in tprms_to_delete:
                session.delete(tprm_to_delete)

            # prm delete
            tprms_ids_to_delete = set()
            for tprm in tprms_to_delete:
                tprms_ids_to_delete.add(tprm.id)
                tprms_ids_to_delete.add(tprm.backward_link)
            stmt = select(PRM).where(PRM.tprm_id.in_(tprms_ids_to_delete))
            chunk_size = 1000
            session.info["disable_security"] = True
            for prm in session.execute(stmt).yield_per(chunk_size).scalars():
                session.delete(prm)
        if tprms_to_create:
            another_tprms = create_another_tprm_id_and_update_existed(
                session=session, tprms=tprms_to_create
            )
            for another_tprm in another_tprms:
                if another_tprm.id in updated_tprm_ids:
                    continue
                updated_tprms.append(another_tprm)
        if updated_tprms:
            session.flush(updated_tprms)
            for updated_tprm in updated_tprms:
                session.refresh(updated_tprm)

    except IntegrityError:
        session.rollback()
        print(traceback.format_exc())
        msg = "Failed to create tprm."
        for tprm in tprms:
            update_tprm_with_id = TprmUpdateWithId.from_orm(tprm)
            error_msg = TprmErrorMsg(msg=msg, tprm=update_tprm_with_id)
            errors.append(error_msg)
        return errors, tprms
    else:
        return errors, updated_tprms


def format_error(error: TprmErrorMsg):
    return (
        f"{error.msg}. TPRM ID: {error.tprm.id}, TPRM name: {error.tprm.name}"
    )


def update_two_way_mo_link_tprms(
    session: Session,
    update_tprms: dict[TprmId, TPRMUpdate],
    in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
) -> tuple[list[str], list[TPRMUpdate | TPRM]]:
    errors, cleared_tprms = check_update_two_way_tprms(
        session=session, update_tprms=update_tprms
    )
    if errors and in_case_of_error == ErrorHandlingType.RAISE_ERROR:
        raise ValueError("\n".join(map(format_error, errors)))

    formatted_errors = [format_error(error=err) for err in errors]
    if in_case_of_error == ErrorHandlingType.ONLY_CHECKING:
        return formatted_errors, cleared_tprms

    additional_errors, created_tprms = update_cleared_two_way_mo_link_tprms(
        tprms=cleared_tprms, session=session, update_tprms=update_tprms
    )
    if created_tprms:
        session.commit()
    additional_errors = [format_error(error=err) for err in additional_errors]
    formatted_errors.extend(additional_errors)
    return formatted_errors, created_tprms
