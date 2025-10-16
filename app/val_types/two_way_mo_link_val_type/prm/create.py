import dataclasses
import traceback
from collections import defaultdict
from typing import TypeAlias

from sqlalchemy import select, and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import PRM, TPRM, MO
from routers.parameter_router.schemas import PRMCreateByMO
from val_types.constants import two_way_mo_link_val_type_name, ErrorHandlingType
from val_types.two_way_mo_link_val_type.tprm.create import TprmId

MoId: TypeAlias = int
PrmId: TypeAlias = int


@dataclasses.dataclass(slots=True)
class PrmErrorMsg:
    msg: str
    prm: PRM


@dataclasses.dataclass(slots=True, frozen=True)
class PrmAndBackwardPrm:
    prm: PRM
    backward_prm: PRM


@dataclasses.dataclass(frozen=True, slots=True)
class RemovePrmFrozenData:
    mo_id: int
    tprm_id: int
    value: int


def check_create_two_way_prms(
    session: Session,
    new_prms: dict[MoId, list[PRMCreateByMO]],
    tprms: dict[TprmId, TPRM],
    mos: dict[MoId, MO] | None = None,
) -> tuple[list[PrmErrorMsg], list[PRM]]:
    errors: list[PrmErrorMsg] = []
    cleared_prms = []
    remove_from_cleared = set()

    backward_tprm_ids: list[TprmId] = [
        i.backward_link for i in tprms.values() if i.backward_link
    ]
    backward_tprms_stmt = select(TPRM).where(TPRM.id.in_(backward_tprm_ids))
    backward_tprms = {
        i.id: i for i in session.execute(backward_tprms_stmt).scalars()
    }
    all_tprms: dict[TprmId, TPRM] = backward_tprms.copy()
    all_tprms.update(tprms)

    if not mos:
        mos_stmt = select(MO).where(MO.id.in_(new_prms.keys()))
        mos = {i.id: i for i in session.execute(mos_stmt).scalars()}

    linked_mo_ids: dict[MoId, list[PrmAndBackwardPrm]] = defaultdict(list)
    for mo_id, new_prms_list in new_prms.items():
        mo: MO | None = mos.get(mo_id, None)
        prms_by_tprms: dict[TprmId, list[PRM]] = defaultdict(list)
        for new_prm in new_prms_list:
            prm_errors: list[PrmErrorMsg] = []
            str_value = (
                str(new_prm.value)
                if isinstance(new_prm.value, int)
                else new_prm.value
            )
            new_prm_converted = PRM(
                value=str_value, mo_id=mo_id, tprm_id=new_prm.tprm_id
            )
            tprm: TPRM | None = all_tprms.get(new_prm.tprm_id, None)

            prms_by_tprms[new_prm.tprm_id].append(new_prm_converted)
            prms_list = prms_by_tprms[new_prm.tprm_id]
            if len(prms_list) == 2:
                for prm in prms_list:
                    msg = "Same type params cannot be created for single object"
                    error_msg = PrmErrorMsg(msg=msg, prm=prm)
                    prm_errors.append(error_msg)
            elif len(prms_list) > 2:
                msg = "Same type params cannot be created for single object"
                error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                prm_errors.append(error_msg)

            if not mo:
                msg = "MO not found"
                error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                prm_errors.append(error_msg)
            if not tprm:
                msg = "TPRM not found"
                error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                prm_errors.append(error_msg)
            if mo and tprm and mo.tmo_id != tprm.tmo_id:
                msg = "PRM tmo_id and TPRM tmo_id mismatch"
                error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                prm_errors.append(error_msg)
            if tprm:
                backward_tprm: TPRM | None = backward_tprms.get(
                    tprm.backward_link, None
                )
                if not backward_tprm:
                    msg = "Backward TPRM not found"
                    error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                    prm_errors.append(error_msg)
            else:
                backward_tprm = None
            if not new_prm.value:
                msg = "Value not found"
                error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                prm_errors.append(error_msg)

            backward_mo_id = None
            if isinstance(new_prm.value, str):
                if not new_prm.value.isdigit():
                    msg = "Value must be an integer"
                    error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                    prm_errors.append(error_msg)
                else:
                    try:
                        backward_mo_id = int(new_prm.value)
                    except ValueError:
                        msg = "Value cannot be converted to an integer"
                        error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                        prm_errors.append(error_msg)
            elif isinstance(new_prm.value, int):
                backward_mo_id = new_prm.value
            else:
                msg = "Value cannot be converted to an integer"
                error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                prm_errors.append(error_msg)

            if backward_mo_id and backward_tprm:
                backward_prm = PRM(
                    value=str(mo_id),
                    mo_id=backward_mo_id,
                    tprm_id=backward_tprm.id,
                )
                linked_mo_ids[backward_mo_id].append(
                    PrmAndBackwardPrm(
                        prm=new_prm_converted, backward_prm=backward_prm
                    )
                )
                backward_mo_stmt = select(MO).where(MO.id == backward_mo_id)
                backward_mo = session.execute(
                    backward_mo_stmt
                ).scalar_one_or_none()
                if not backward_mo:
                    msg = "Mo by value not found"
                    error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                    prm_errors.append(error_msg)
                elif backward_tprm.tmo_id != backward_mo.tmo_id:
                    msg = "Constraint tmo_id and linked value tmo_id mismatch"
                    error_msg = PrmErrorMsg(msg=msg, prm=new_prm_converted)
                    prm_errors.append(error_msg)
            if prm_errors:
                errors.extend(prm_errors)
            else:
                cleared_prms.append(new_prm_converted)
        check_link_existing_stmt = select(PRM.tprm_id).where(
            PRM.mo_id == mo_id, PRM.tprm_id.in_(prms_by_tprms.keys())
        )
        for existing_mo_tprm_id in session.execute(
            check_link_existing_stmt
        ).scalars():
            for prm in prms_by_tprms[existing_mo_tprm_id]:  # type: PRM
                msg = "Multiple references to a single object are not allowed for this type. PRM already exists."
                error_msg = PrmErrorMsg(msg=msg, prm=prm)
                errors.append(error_msg)
                remove_obj = RemovePrmFrozenData(
                    mo_id=mo_id, tprm_id=prm.tprm_id, value=int(prm.value)
                )
                remove_from_cleared.add(remove_obj)
    stmt_constraints = []
    for mo_id, prms in linked_mo_ids.items():
        if len(prms) > 1:
            for prm in prms:  # type: PrmAndBackwardPrm
                msg = "Multiple references to a single object are not allowed for this type"
                error_msg = PrmErrorMsg(msg=msg, prm=prm.prm)
                errors.append(error_msg)
                remove_obj = RemovePrmFrozenData(
                    mo_id=mo_id,
                    tprm_id=prm.prm.tprm_id,
                    value=int(prm.prm.value),
                )
                remove_from_cleared.add(remove_obj)
            continue
        prm: PrmAndBackwardPrm = prms[0]
        stmt_constraint = and_(
            PRM.mo_id == mo_id, PRM.tprm_id == prm.backward_prm.tprm_id
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
            for prm in prms:  # type: PrmAndBackwardPrm
                if (
                    prm.backward_prm.mo_id,
                    prm.backward_prm.tprm_id,
                ) not in exclude:
                    continue
                msg = "Multiple references to a single object are not allowed for this type. Backward"
                error_msg = PrmErrorMsg(msg=msg, prm=prm.prm)
                errors.append(error_msg)
                remove_obj = RemovePrmFrozenData(
                    mo_id=prm.prm.mo_id,
                    tprm_id=prm.prm.tprm_id,
                    value=int(prm.prm.value),
                )
                remove_from_cleared.add(remove_obj)
    if remove_from_cleared:
        cleared_prms = [
            i
            for i in cleared_prms
            if RemovePrmFrozenData(
                mo_id=i.mo_id, tprm_id=i.tprm_id, value=int(i.value)
            )
            not in remove_from_cleared
        ]
    return errors, cleared_prms


def create_another_to_way_mo_link_prm(
    session: Session, prms: list[PRM], tprms: dict[TprmId, TPRM]
) -> list[PRM]:
    another_prms: list[PRM] = []
    prms_by_id = {i.id: i for i in prms}
    for prm in prms:
        tprm = tprms[prm.tprm_id]
        another_prm = PRM(
            mo_id=int(prm.value),
            tprm_id=tprm.backward_link,
            value=str(prm.mo_id),
            backward_link=prm.id,
        )
        another_prms.append(another_prm)
    session.add_all(another_prms)
    session.flush()
    for another_prm in another_prms:
        session.refresh(another_prm)
        prm = prms_by_id[another_prm.backward_link]
        prm.backward_link = another_prm.id
    session.flush()
    for prm in prms:
        session.refresh(prm)
    return another_prms


def create_cleared_two_way_mo_link_prms(
    session: Session, new_prms: list[PRM], tprms: dict[TprmId, TPRM]
) -> tuple[list[PrmErrorMsg], list[PRM]]:
    errors: list[PrmErrorMsg] = []
    prms: list[PRM] = []
    if not new_prms:
        return errors, prms

    session.add_all(new_prms)
    try:
        session.flush()
        for new_prm in new_prms:
            session.refresh(new_prm)
        another_prms = create_another_to_way_mo_link_prm(
            session=session, prms=new_prms, tprms=tprms
        )
    except IntegrityError:
        session.rollback()
        print(traceback.format_exc())
        msg = "Failed to create prm."
        for prm in new_prms:
            error_msg = PrmErrorMsg(msg=msg, prm=prm)
            errors.append(error_msg)
    else:
        prms.extend(new_prms)
        prms.extend(another_prms)
    return errors, prms


def format_error(error: PrmErrorMsg):
    return (
        f"{error.msg}. MO ID: {error.prm.mo_id}, TPRM ID: {error.prm.tprm_id}"
    )


def create_two_way_mo_link_prms(
    session: Session,
    new_parameter_types: dict[MoId, list[PRMCreateByMO]],
    parameter_types: list[TPRM] | None,
    in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
    autocommit: bool = True,
):
    if parameter_types is None:
        tprm_ids = set()
        for mo, list_new_prms in new_parameter_types.items():
            tprm_ids.update([i.tprm_id for i in list_new_prms])
        tprms_stmt = select(TPRM).where(
            TPRM.id.in_(tprm_ids),
            TPRM.val_type == two_way_mo_link_val_type_name,
        )
        two_way_mo_link_tprms = {
            i.id: i for i in session.execute(tprms_stmt).scalars()
        }
    else:
        two_way_mo_link_tprms = {
            i.id: i
            for i in parameter_types
            if i.val_type == two_way_mo_link_val_type_name
        }

    errors, cleared_new_prms = check_create_two_way_prms(
        session=session,
        new_prms=new_parameter_types,
        tprms=two_way_mo_link_tprms,
    )
    if errors and in_case_of_error == ErrorHandlingType.RAISE_ERROR:
        raise ValueError("\n".join(map(format_error, errors)))

    formatted_errors = [format_error(error=err) for err in errors]
    if in_case_of_error == ErrorHandlingType.ONLY_CHECKING:
        return formatted_errors, cleared_new_prms
    additional_errors, created_prms = create_cleared_two_way_mo_link_prms(
        new_prms=cleared_new_prms, session=session, tprms=two_way_mo_link_tprms
    )
    if created_prms:
        if autocommit:
            session.commit()
        for created_prm in created_prms:
            session.refresh(created_prm)
    additional_errors = [format_error(error=err) for err in additional_errors]
    formatted_errors.extend(additional_errors)
    return formatted_errors, created_prms
