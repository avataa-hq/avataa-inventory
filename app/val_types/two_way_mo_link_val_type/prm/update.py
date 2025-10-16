import dataclasses
import traceback
from collections import defaultdict

from sqlalchemy import select, and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import PRM, MO, TPRM
from routers.parameter_router.schemas import PRMUpdateByMO
from val_types.constants import ErrorHandlingType
from val_types.two_way_mo_link_val_type.prm.create import (
    PrmErrorMsg,
    format_error,
    MoId,
    PrmId,
    create_another_to_way_mo_link_prm,
)
from val_types.two_way_mo_link_val_type.tprm.create import TprmId
from val_types.constants import two_way_mo_link_val_type_name


@dataclasses.dataclass(slots=True)
class PrmAndNewMoId:
    prm: PRM
    mo_id: MoId


def check_update_two_way_prms(
    session: Session,
    update_prms: dict[MoId, list[PRM]],
    tprms_by_tprm_id: dict[TprmId, TPRM],
) -> tuple[list[PrmErrorMsg], list[PRM]]:
    errors: list[PrmErrorMsg] = []
    cleared_prms = []
    if not update_prms:
        return errors, cleared_prms

    mos_stmt = select(MO).where(MO.id.in_(update_prms.keys()))
    mos_by_mo_id = {i.id: i for i in session.execute(mos_stmt).scalars()}

    prm_ids: set[PrmId] = set()
    for prms_list in update_prms.values():
        for prm in prms_list:
            prm_ids.add(prm.id)

    prms_stmt = (
        select(PRM)
        .join(TPRM, PRM.tprm_id == TPRM.id)
        .where(
            PRM.id.in_(prm_ids), TPRM.val_type == two_way_mo_link_val_type_name
        )
    )
    prms_by_prm_id = {i.id: i for i in session.execute(prms_stmt).scalars()}

    remove_from_cleared: set[PrmId] = set()
    linked_mo_ids: dict[MoId, list[PRM]] = defaultdict(list)
    prms_by_backward_id: dict[PrmId, PRM] = {}
    for mo_id, update_prms_list in update_prms.items():
        mo = mos_by_mo_id.get(mo_id, None)
        prms_by_prm_id_duplication_check: dict[PrmId, list[PRM]] = defaultdict(
            list
        )
        for update_prm in update_prms_list:  # type: PRM
            prm_errors: list[PrmErrorMsg] = []

            prms_by_prm_id_duplication_check[update_prm.id].append(update_prm)
            prms_list = prms_by_prm_id_duplication_check[update_prm.id]
            if len(prms_list) == 2:
                for prm in prms_list:
                    msg = "The same parameter cannot be updated more than once in the same operation"
                    error_msg = PrmErrorMsg(msg=msg, prm=prm)
                    prm_errors.append(error_msg)
                    remove_from_cleared.add(prm.id)
            elif len(prms_list) > 2:
                msg = "The same parameter cannot be updated more than once in the same operation"
                error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                prm_errors.append(error_msg)
            if not mo:
                msg = "MO ID {0} does not exist".format(update_prm.mo_id)
                error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                prm_errors.append(error_msg)
            existing_prm: PRM | None = prms_by_prm_id.get(update_prm.id)
            if not existing_prm:
                msg = 'PRM ID {0} does not exist or type is not "{1}"'.format(
                    update_prm.id, two_way_mo_link_val_type_name
                )
                error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                prm_errors.append(error_msg)
            else:
                if update_prm.version != existing_prm.version:
                    msg = "Version mismatch"
                    error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                    prm_errors.append(error_msg)
                prms_by_backward_id[existing_prm.backward_link] = existing_prm
                if existing_prm.id in prms_by_backward_id:
                    msg = "Attempting to change both related parameters"
                    error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                    prm_errors.append(error_msg)

                    another_prm_update = prms_by_backward_id[existing_prm.id]
                    error_msg = PrmErrorMsg(msg=msg, prm=another_prm_update)
                    prm_errors.append(error_msg)
                    remove_obj = another_prm_update.id
                    remove_from_cleared.add(remove_obj)
            if not update_prm.value:
                msg = "Value not exist"
                error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                prm_errors.append(error_msg)
            elif not update_prm.value.isdigit():
                msg = "Cannot convert value {0} to integer".format(
                    update_prm.value
                )
                error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                prm_errors.append(error_msg)
            else:
                try:
                    value_mo_id = int(update_prm.value)
                except ValueError:
                    msg = "Cannot convert value {0} to integer".format(
                        update_prm.value
                    )
                    error_msg = PrmErrorMsg(msg=msg, prm=update_prm)
                    prm_errors.append(error_msg)
                else:
                    if existing_prm:
                        linked_mo_ids[value_mo_id].append(update_prm)
            if prm_errors:
                errors.extend(prm_errors)
            else:
                update_prm = PRM.from_orm(
                    existing_prm, update=update_prm.dict(exclude_unset=True)
                )
                cleared_prms.append(update_prm)

    stmt_constraints = []
    for mo_id, prms_list in linked_mo_ids.items():
        if len(prms_list) > 1:
            for prm in prms_list:  # type: PRM
                msg = "Multiple references to a single object are not allowed for this type"
                error_msg = PrmErrorMsg(msg=msg, prm=prm)
                errors.append(error_msg)
                remove_obj = prm.id
                remove_from_cleared.add(remove_obj)
            continue
        prm: PRM = prms_list[0]
        tprm: TPRM | None = tprms_by_tprm_id.get(prm.tprm_id, None)
        if tprm:
            stmt_constraint = and_(
                PRM.mo_id == mo_id, PRM.tprm_id == tprm.backward_link
            )
            stmt_constraints.append(stmt_constraint)
    if stmt_constraints:
        stmt = select(PRM).where(or_(*stmt_constraints))
        exclude = set()
        for prm in session.execute(stmt).scalars():
            backward_mo_id = prm.mo_id
            backward_tprm_id = prm.tprm_id
            exclude.add((backward_mo_id, backward_tprm_id))
        for mo_id, prms in linked_mo_ids.items():
            for prm in prms:  # type: PRM
                tprm: TPRM | None = tprms_by_tprm_id.get(prm.tprm_id, None)
                if not tprm:
                    continue
                if (int(prm.value), tprm.backward_link) not in exclude:
                    continue
                msg = "Multiple references to a single object are not allowed for this type. Backward"
                error_msg = PrmErrorMsg(msg=msg, prm=prm)
                errors.append(error_msg)
                remove_obj = prm.id
                remove_from_cleared.add(remove_obj)
    if remove_from_cleared:
        cleared_prms = [
            i for i in cleared_prms if i.id not in remove_from_cleared
        ]
    return errors, cleared_prms


def update_cleared_two_way_mo_link_prms(
    session: Session, update_prms: list[PRM], tprms: dict[TprmId, TPRM]
) -> tuple[list[PrmErrorMsg], list[PRM]]:
    errors: list[PrmErrorMsg] = []
    prms: list[PRM] = []
    if not update_prms:
        return errors, prms

    old_backward_prm_ids = []
    for prm in update_prms:
        backward_prm_id = prm.backward_link
        old_backward_prm_ids.append(backward_prm_id)
        prm.backward_link = None
        prm.version += 1
    old_backward_prm_stmt = select(PRM).where(PRM.id.in_(old_backward_prm_ids))
    old_backward_prms = session.execute(old_backward_prm_stmt).scalars().all()

    prms_by_id = {i.id: i for i in update_prms}
    stmt_update_prms = select(PRM).where(PRM.id.in_(prms_by_id.keys()))

    updated_prms = []
    for prm_to_update in session.execute(stmt_update_prms).scalars():
        update = prms_by_id[prm_to_update.id]
        for attr, value in update.dict(exclude_unset=True).items():
            if hasattr(prm_to_update, attr):
                setattr(prm_to_update, attr, value)
        updated_prms.append(prm_to_update)

    session.add_all(updated_prms)
    for old_backward_prm in old_backward_prms:
        session.delete(old_backward_prm)
    try:
        another_prms = create_another_to_way_mo_link_prm(
            session=session, prms=updated_prms, tprms=tprms
        )
    except IntegrityError:
        session.rollback()
        print(traceback.format_exc())
        msg = "Failed to create prm."
        for prm in updated_prms:
            error_msg = PrmErrorMsg(msg=msg, prm=prm)
            errors.append(error_msg)
    else:
        prms.extend(updated_prms)
        prms.extend(another_prms)
    return errors, prms


def update_two_way_mo_link_prms(
    session: Session,
    update_prms: dict[MoId, list[PRMUpdateByMO]],
    tprms: list[TPRM],
    in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
    autocommit: bool = True,
) -> tuple[list[str], list[PRM]]:
    converted_update_prms = convert_prm_update_to_prm(
        update_prms=update_prms, session=session
    )
    two_way_mo_link_tprms = {
        i.id: i for i in tprms if i.val_type == two_way_mo_link_val_type_name
    }
    errors, cleared_prms = check_update_two_way_prms(
        session=session,
        update_prms=converted_update_prms,
        tprms_by_tprm_id=two_way_mo_link_tprms,
    )
    if errors and in_case_of_error == ErrorHandlingType.RAISE_ERROR:
        raise ValueError("\n".join(map(format_error, errors)))

    formatted_errors = [format_error(error=err) for err in errors]
    if in_case_of_error == ErrorHandlingType.ONLY_CHECKING:
        return formatted_errors, cleared_prms

    additional_errors, created_prms = update_cleared_two_way_mo_link_prms(
        update_prms=cleared_prms, session=session, tprms=two_way_mo_link_tprms
    )
    if created_prms:
        if autocommit:
            session.commit()
        for created_prm in created_prms:
            session.refresh(created_prm)
    additional_errors = [format_error(error=err) for err in additional_errors]
    formatted_errors.extend(additional_errors)
    return formatted_errors, created_prms


def convert_prm_update_to_prm(
    update_prms: dict[MoId, list[PRMUpdateByMO]], session: Session
) -> dict[MoId, list[PRM]]:
    results: dict[MoId, list[PRM]] = {}
    if not update_prms:
        return results
    constraints = []
    for mo_id, list_of_prm_updates in update_prms.items():
        for prm_update in list_of_prm_updates:
            constraint = and_(
                PRM.mo_id == mo_id, PRM.tprm_id == prm_update.tprm_id
            )
            constraints.append(constraint)
    # mo_id: tprm_id: prm_id
    tprm_ids_by_mo_id: dict[MoId, dict[TprmId, PrmId]] = defaultdict(dict)
    stmt = select(PRM).where(or_(*constraints))
    for db_prm in session.execute(stmt).scalars():  # type: PRM
        tprm_ids_by_mo_id[db_prm.mo_id][db_prm.tprm_id] = db_prm.id
    # convert
    for mo_id, list_of_prm_updates in update_prms.items():
        new_list = []
        for prm_update in list_of_prm_updates:
            prm_id = tprm_ids_by_mo_id.get(mo_id, {}).get(
                prm_update.tprm_id, None
            )
            new_prm = PRM.from_orm(
                prm_update, update={"id": prm_id, "mo_id": mo_id}
            )
            new_list.append(new_prm)
        results[mo_id] = new_list
    return results
