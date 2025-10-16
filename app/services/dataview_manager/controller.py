from itertools import groupby
from typing import Generator

from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from models import MO, PRM


class GrpcController:
    @staticmethod
    def get_objects(
        session: Session,
        tmo_id: int,
    ) -> Generator:
        QUERY_SIZE = 30000  # noqa
        select_statement = select(
            MO.id.label("id"), MO.name.label("parent_name")
        )
        aliased_table = aliased(select_statement.subquery())

        stmt_point_a = select(MO.id.label("id"), MO.name.label("point_a_name"))
        al_table_point_a = aliased(stmt_point_a.subquery())

        stmt_point_b = select(MO.id.label("id"), MO.name.label("point_b_name"))
        al_table_point_b = aliased(stmt_point_b.subquery())

        offset = 0
        while True:
            query = select(
                MO.id,
                MO.p_id,
                MO.tmo_id,
                MO.name,
                MO.pov,
                MO.geometry,
                MO.document_count,
                MO.point_a_id,
                MO.point_b_id,
                MO.model,
                MO.version,
                MO.active,
                MO.latitude,
                MO.longitude,
                MO.label,
                MO.status,
                MO.creation_date,
                MO.modification_date,
            ).where(MO.tmo_id == tmo_id)
            query = (
                query.outerjoin(aliased_table, MO.p_id == aliased_table.c.id)
                .outerjoin(
                    al_table_point_a, MO.point_a_id == al_table_point_a.c.id
                )
                .outerjoin(
                    al_table_point_b, MO.point_b_id == al_table_point_b.c.id
                )
                .add_columns(
                    aliased_table.c.parent_name,
                    al_table_point_a.c.point_a_name,
                    al_table_point_b.c.point_b_name,
                )
            )
            query = query.order_by(MO.id)
            query = query.limit(QUERY_SIZE).offset(offset)
            result = session.execute(query)
            result = {item[0]: item._asdict() for item in result.fetchall()}
            if len(result) == 0:
                break

            yield result
            offset += QUERY_SIZE

    @staticmethod
    def replace_links(
        session: Session,
        objects: Generator,
        mo_links: list,
        prm_links: list,
    ) -> Generator:
        for mos in objects:
            mo_link_values = dict()
            prm_link_values = dict()

            for mo_id, item in mos.items():
                params = item.get("params", [])

                # grab mo_links
                mo_links_raw = [
                    param["value"]
                    for param in params
                    if param["tprm_id"] in mo_links
                    and param["value"] is not None
                    and param["value"] not in mo_link_values.keys()
                ]
                query = select(MO.id, MO.name).where(MO.id.in_(mo_links_raw))
                mo_links_raw = session.execute(query)
                mo_links_raw = {str(lv[0]): lv[1] for lv in mo_links_raw}
                mo_link_values.update(mo_links_raw)

                # grab prm_links
                prm_links_raw = [
                    param["value"]
                    for param in params
                    if param["tprm_id"] in prm_links
                    and param["value"] is not None
                    and param["value"] not in prm_link_values.keys()
                ]
                query = select(PRM.id, PRM.value).where(
                    PRM.id.in_(prm_links_raw)
                )
                prm_links_raw = session.execute(query)
                prm_links_raw = {str(lv[0]): lv[1] for lv in prm_links_raw}
                prm_link_values.update(prm_links_raw)

                # replace links
                for param in params:
                    if param["tprm_id"] in mo_links and param["value"]:
                        param["value"] = mo_link_values[param["value"]]
                    if param["tprm_id"] in prm_links and param["value"]:
                        params["value"] = prm_link_values[param["value"]]

                mos[mo_id]["params"] = params

            yield mos

    @staticmethod
    def get_parameters(session: Session, objects: Generator):
        # TODO add type convertation
        for mos in objects:
            query = (
                select(PRM.mo_id, PRM.value, PRM.tprm_id)
                .where(PRM.mo_id.in_(mos.keys()))
                .order_by(PRM.mo_id)
            )
            result = session.execute(query)
            result = [item._asdict() for item in result.fetchall()]

            iterator = groupby(result, key=lambda x: x.pop("mo_id"))

            for key, group in iterator:
                mos[key]["params"] = list(group)

            yield mos
