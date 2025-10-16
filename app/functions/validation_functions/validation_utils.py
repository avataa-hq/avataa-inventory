import pickle

from sqlmodel import Session, select

from functions.db_functions import db_read
from functions.functions_utils import utils
from models import TPRM, PRM
from routers.parameter_router.schemas import PRMCreateByMO


def update_parameter_multiple_value_for_str(
    session: Session, multiple_value, prm_id: int
):
    session.info["disable_security"] = True
    db_param = db_read.get_db_param_or_exception(session=session, prm_id=prm_id)
    new_value = pickle.dumps(multiple_value).hex()
    db_param.value = new_value
    session.add(db_param)


def delete_parameter_by_prm_link_multiple_value(
    session: Session, prm_id: int, param
):
    session.info["disable_security"] = True
    prm_link_tprms = session.exec(
        select(TPRM).where(TPRM.val_type == "prm_link")
    ).all()
    for prm_link_tprm in prm_link_tprms:
        for prm_link in prm_link_tprm.prms:
            multiple_value = utils.decode_multiple_value(value=prm_link.value)
            if prm_id in multiple_value:
                session.delete(prm_link)


def delete_parameter_by_prm_link_value(session: Session, param):
    session.info["disable_security"] = True
    prm_link_tprms = session.exec(
        select(TPRM).where(TPRM.val_type == "prm_link")
    ).all()
    for prm_link_tprm in prm_link_tprms:
        for prm_link in prm_link_tprm.prms:
            if prm_link_tprm.multiple:
                prm_link_value = pickle.loads(bytes.fromhex(prm_link.value))
                prm_link_value = [
                    value for value in prm_link_value if value != param.id
                ]
                if prm_link_value:
                    prm_link.value = pickle.dumps(prm_link_value).hex()
                    session.add(prm_link)
                else:
                    session.delete(prm_link)
            else:
                if int(prm_link.value) == param.id:
                    session.delete(prm_link)


def update_parameter_bool_value(session: Session, status: str, param):
    session.info["disable_security"] = True
    db_param = db_read.get_db_param_or_exception(
        session=session, prm_id=param.id
    )
    new_value = status
    db_param.value = new_value
    session.add(db_param)


def get_possible_prm_ids_for_internal_link(
    session: Session,
    external_tprm_id: int,
    internal_parameter_link: PRM | PRMCreateByMO,
    db_param_type: TPRM,
) -> list[int]:
    linked_param_value = session.get(
        PRM, int(internal_parameter_link.value)
    ).value

    stmt = select(PRM.mo_id).where(
        PRM.tprm_id == int(external_tprm_id), PRM.value == linked_param_value
    )
    possible_mo_ids = session.execute(stmt).scalars().all()

    stmt = select(PRM.id).where(
        PRM.tprm_id == int(db_param_type.constraint),
        PRM.mo_id.in_(possible_mo_ids),
    )
    possible_prm_ids = session.execute(stmt).scalars().all()
    return possible_prm_ids


def get_possible_prm_ids_for_external_link(
    session: Session, external_tprm_id: int, db_param_type: TPRM
):
    linked_mo_ids = session.exec(
        select(PRM.mo_id).where(PRM.tprm_id == int(db_param_type.constraint))
    ).all()
    external_mo_ids = session.exec(
        select(PRM.mo_id).where(PRM.tprm_id == int(external_tprm_id))
    ).all()

    possible_mo_ids = set(linked_mo_ids).difference(set(external_mo_ids))

    stmt = select(PRM.id).where(
        PRM.tprm_id == int(db_param_type.constraint),
        PRM.mo_id.in_(possible_mo_ids),
    )

    possible_prm_ids = session.execute(stmt).scalars().all()
    return possible_prm_ids
