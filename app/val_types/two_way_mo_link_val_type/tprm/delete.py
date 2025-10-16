from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TPRM
from val_types.constants import ErrorHandlingType
from val_types.two_way_mo_link_val_type.tprm.create import (
    TprmId,
    two_way_mo_link_val_type_name,
)


def check_delete_two_way_tprms(
    session: Session, tprm_ids: list[TprmId]
) -> tuple[list[str], list[TPRM]]:
    errors = []
    tprms_stmt = select(TPRM).where(
        TPRM.val_type == two_way_mo_link_val_type_name, TPRM.id.in_(tprm_ids)
    )
    if tprm_ids:
        tprms: list[TPRM] = session.execute(tprms_stmt).scalars().all()
        if tprms:
            enabled_tprm_ids = [i.id for i in tprms]
            backward_tprms_stmt = select(TPRM).where(
                TPRM.val_type == two_way_mo_link_val_type_name,
                TPRM.backward_link.in_(enabled_tprm_ids),
            )
            session.info["disable_security"] = True
            backward_tprms: list[TPRM] = (
                session.execute(backward_tprms_stmt).scalars().all()
            )
            tprms.extend(backward_tprms)
    else:
        tprms: list[TPRM] = []
    tprm_ids_set: set[TprmId] = set(tprm_ids)
    existing_tprm_ids: set[TprmId] = set(i.id for i in tprms)
    not_existing_tprm_ids = tprm_ids_set.difference(existing_tprm_ids)
    for tprm_id in not_existing_tprm_ids:
        msg = (
            'Tprm with ID {0} was not found or value type is not "{1}".'.format(
                tprm_id, two_way_mo_link_val_type_name
            )
        )
        errors.append(msg)
    return errors, tprms


def delete_tprms(
    session: Session, tprms: list[TPRM]
) -> tuple[list[str], list[TPRM]]:
    errors: list[str] = []
    tprms_copy = [tprm.copy(deep=True) for tprm in tprms]
    for tprm in tprms:
        session.delete(tprm)

    session.flush()
    return errors, tprms_copy


def delete_two_way_mo_link_tprms(
    session: Session,
    tprm_ids: list[TprmId],
    in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
) -> tuple[list[str], list[TPRM]]:
    errors, tprms = check_delete_two_way_tprms(
        session=session, tprm_ids=tprm_ids
    )
    if errors and in_case_of_error == ErrorHandlingType.RAISE_ERROR:
        raise ValueError("\n".join(errors))
    if in_case_of_error == ErrorHandlingType.ONLY_CHECKING:
        return errors, tprms
    additional_errors, tprms = delete_tprms(session=session, tprms=tprms)
    if tprms:
        session.commit()
    errors.extend(additional_errors)
    return errors, tprms
