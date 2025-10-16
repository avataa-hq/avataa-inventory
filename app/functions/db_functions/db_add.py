from typing import Any

from sqlmodel import select, Session

from models import PRM, TPRM, MO


def add_required_params_for_objects_when_update_param_type(
    session: Session, db_param_type: TPRM, field_value: Any
) -> None:
    db_objects = session.exec(
        select(MO).where(MO.tmo_id == db_param_type.tmo_id)
    ).all()

    obj_ids = [o.id for o in db_objects]
    db_params = session.exec(
        select(PRM).where(
            PRM.mo_id.in_(obj_ids), PRM.tprm_id == db_param_type.id
        )
    ).all()
    obj_param_exist_ids = [p.mo_id for p in db_params]
    obj_param_need_ids = [x for x in obj_ids if x not in obj_param_exist_ids]
    for obj_id in obj_param_need_ids:
        param = PRM(tprm_id=db_param_type.id, mo_id=obj_id, value=field_value)
        session.add(param)
        session.commit()
        session.refresh(param)


def add_required_params_for_objects_when_create_param_type(
    session: Session, db_param_type: TPRM, field_value: Any
) -> None:
    session.info["disable_security"] = True
    db_objects = session.exec(
        select(MO).where(MO.tmo_id == db_param_type.tmo_id)
    ).all()
    for db_object in db_objects:
        param = PRM(
            tprm_id=db_param_type.id, mo_id=db_object.id, value=field_value
        )
        session.add(param)
        session.commit()
        session.refresh(param)
