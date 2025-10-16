import json  # noqa

from sqlmodel import select, Session

from functions.db_functions.db_read import get_db_object_or_exception
from models import MO, PRM


def filter_objects_by_parameters(
    session: Session,
    data,
    object_type_id: int = None,
    mos_ids: list = None,
    p_id: int = None,
) -> list:
    if p_id is not None:
        get_db_object_or_exception(session=session, object_id=p_id)
    wheres = []
    for filter_ in data:
        contains = filter_["operator"] == "contains"
        for value in filter_["values"]:
            where = (
                PRM.tprm_id == filter_["id"],
                PRM.value.like("%" + value + "%")
                if contains
                else PRM.value == value,
                MO.tmo_id == object_type_id
                if object_type_id is not None
                else True,
                MO.id.in_(mos_ids) if mos_ids is not None else True,
                MO.p_id == p_id if p_id is not None else True,
            )
            wheres.extend(where)
    wheres = set(wheres)
    mo_ids_query = select(PRM).join(MO).where(*wheres)
    prms = session.exec(mo_ids_query).all()
    mo_ids = set([prm.mo_id for prm in prms])
    return mo_ids
