from sqlalchemy import select
from sqlalchemy.orm import Session

from models import PRM, TPRM
from val_types.two_way_mo_link_val_type.prm.create import PrmId
from val_types.constants import two_way_mo_link_val_type_name, ErrorHandlingType


def check_delete_two_way_prms(
    session: Session, prm_ids: list[PrmId]
) -> tuple[list[str], list[PRM]]:
    errors = []
    prms_stmt = (
        select(PRM)
        .join(TPRM, PRM.tprm_id == TPRM.id)
        .filter(
            PRM.id.in_(prm_ids), TPRM.val_type == two_way_mo_link_val_type_name
        )
    )
    if prm_ids:
        prms: list[PRM] = session.execute(prms_stmt).scalars().all()
        if prms:
            enabled_prm_ids = [i.id for i in prms]
            backward_prms_stmt = select(PRM).where(
                PRM.backward_link.in_(enabled_prm_ids)
            )
            session.info["disable_security"] = True
            backward_prms: list[PRM] = (
                session.execute(backward_prms_stmt).scalars().all()
            )
            prms.extend(backward_prms)
    else:
        prms: list[PRM] = []
    prm_ids_set: set[PrmId] = set(prm_ids)
    existing_prm_ids: set[PrmId] = set(i.id for i in prms)
    not_existing_prm_ids = prm_ids_set.difference(existing_prm_ids)
    for prm_id in not_existing_prm_ids:
        msg = (
            'Prm with ID {0} was not found or value type is not "{1}".'.format(
                prm_id, two_way_mo_link_val_type_name
            )
        )
        errors.append(msg)
    return errors, prms


def delete_prms(
    session: Session, prms: list[PRM]
) -> tuple[list[str], list[PRM]]:
    errors: list[str] = []
    prms_copy = [prm.copy(deep=True) for prm in prms]
    for prm in prms:
        session.delete(prm)
    session.flush()
    return errors, prms_copy


def delete_two_way_mo_link_prms(
    session: Session,
    prm_ids: list[PrmId],
    in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
) -> tuple[list[str], list[PRM]]:
    prms: list[PRM]
    errors, prms = check_delete_two_way_prms(session=session, prm_ids=prm_ids)
    if errors and in_case_of_error == ErrorHandlingType.RAISE_ERROR:
        raise ValueError("\n".join(errors))
    if in_case_of_error == ErrorHandlingType.ONLY_CHECKING:
        return errors, prms
    additional_errors, prms = delete_prms(session=session, prms=prms)
    if prms:
        session.commit()
    errors.extend(additional_errors)
    return errors, prms
