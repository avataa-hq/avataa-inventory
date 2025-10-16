import re
from collections import defaultdict
from typing import List

from fastapi import HTTPException
from sqlalchemy import or_, and_, desc, asc
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from common.common_schemas import OrderByRule
from common.common_utils import unpack_dict_values
from database import SQLALCHEMY_LIMIT
from functions import functions_dicts as func
from functions.functions_utils import utils
from functions.validation_functions.validation_function import (
    get_param_to_read_based_on_multiple,
)
from models import TMO, MO, TPRM, PRM
from routers.parameter_router.schemas import PRMReadMultiple, PRMRead
from routers.parameter_router.utils import (
    get_parameters_of_object,
    get_links_of_parameters,
)


def get_db_object_type_or_exception(
    session: Session, object_type_id: int
) -> TMO:
    db_object_type = session.get(TMO, object_type_id)
    if not db_object_type:
        raise HTTPException(
            status_code=404,
            detail=f"Object type with id {object_type_id} not found.",
        )
    return db_object_type


def get_exists_objects(session: Session, mo_ids: list[int]) -> List[MO] | list:
    objects = session.exec(select(MO).where(MO.id.in_(mo_ids))).all()
    return objects


def get_db_object_or_exception(session: Session, object_id: int) -> MO:
    db_object = session.get(MO, object_id)
    if not db_object:
        raise HTTPException(
            status_code=404, detail=f"Object with id {object_id} not found."
        )
    return db_object


def get_db_param_or_exception(session: Session, prm_id: int) -> PRM:
    db_param = session.get(PRM, prm_id)
    if not db_param:
        raise HTTPException(
            status_code=404, detail=f"Object with id {prm_id} not found."
        )
    return db_param


def get_db_param_type_or_exception_422(session: Session, tprm_id: int) -> TPRM:
    db_param_type = session.get(TPRM, tprm_id)
    if not db_param_type:
        raise HTTPException(
            status_code=422,
            detail=f"Parameter type with id {tprm_id} doesn't exist.",
        )
    return db_param_type


def get_objects_by_object_type_id(
    session: Session, tmo_id: int, limit: int, offset: int
) -> MO:
    if tmo_id is not None:
        objects = session.exec(
            select(MO).where(MO.tmo_id == tmo_id).offset(offset).limit(limit)
        ).all()
    else:
        objects = session.exec(select(MO).offset(offset).limit(limit)).all()
    return objects


def get_object_with_parameters(
    session: Session, db_object: MO, with_parameters: bool = False
) -> dict:
    # if 'with_parameters' we need to add all params to attribute params and 'returnable' params to MO attributes level
    # os if its False - we need to send only returnable params
    object_params = get_parameters_of_object(
        session=session, object_id=db_object.id, only_returnable=with_parameters
    )
    object_type_instance = session.get(TMO, db_object.tmo_id)

    # gather all object (mos\prms) to which we link
    all_linked_mos = get_links_of_parameters(
        parameters=object_params, val_type="mo_link"
    )
    all_linked_prms = get_links_of_parameters(
        parameters=object_params, val_type="prm_link"
    )

    # get names of all mos to which we link
    if all_linked_mos:
        mo_linked_ids: list = [
            int(mo_id)
            for mo_id in set(
                unpack_dict_values(dict_with_mixed_values=all_linked_mos)
            )
        ]
        mo_linked_ids: list[list[MO.id]] = [
            mo_linked_ids[index : index + SQLALCHEMY_LIMIT]
            for index in range(0, len(mo_linked_ids), SQLALCHEMY_LIMIT)
        ]

        linked_mo_id_and_name = {}
        for chunk in mo_linked_ids:
            temp = session.exec(
                select(MO.id, MO.name).where(MO.id.in_(chunk))
            ).all()
            linked_mo_id_and_name.update(
                {mo_id: mo_name for mo_id, mo_name in temp}
            )

        for prm_id, linked_mo_ids in all_linked_mos.items():
            if isinstance(linked_mo_ids, list):
                all_linked_mos[prm_id] = [
                    linked_mo_id_and_name[mo_id] for mo_id in linked_mo_ids
                ]
                continue
            all_linked_mos[prm_id] = linked_mo_id_and_name[int(linked_mo_ids)]

    # get parameters of all prm`s to which we link
    if all_linked_prms:
        prm_linked_ids = list(
            set(unpack_dict_values(dict_with_mixed_values=all_linked_prms))
        )
        prm_linked_ids: list[list[PRM.id]] = [
            prm_linked_ids[index : index + SQLALCHEMY_LIMIT]
            for index in range(0, len(prm_linked_ids), SQLALCHEMY_LIMIT)
        ]
        linked_parameters = []
        for chunk in prm_linked_ids:
            linked_parameters.extend(
                session.exec(select(PRM).where(PRM.id.in_(chunk))).all()
            )

        linked_mo_ids = get_links_of_parameters(
            parameters=linked_parameters, val_type="mo_link"
        )

        if linked_mo_ids:
            mo_linked_ids = list(
                set(unpack_dict_values(dict_with_mixed_values=linked_mo_ids))
            )
            mo_linked_ids: list[list[MO.id]] = [
                mo_linked_ids[index : index + SQLALCHEMY_LIMIT]
                for index in range(0, len(mo_linked_ids), SQLALCHEMY_LIMIT)
            ]

            linked_mo_id_and_name = {}
            for chunk in mo_linked_ids:
                temp = session.exec(
                    select(MO.id, MO.name).where(MO.id.in_(chunk))
                ).all()
                linked_mo_id_and_name.update(
                    {mo_id: mo_name for mo_id, mo_name in temp}
                )

            linked_mo_link_and_his_values = {}
            for prm_id, mo_ids in linked_mo_ids.items():
                if isinstance(linked_mo_ids, list):
                    linked_mo_link_and_his_values[prm_id] = [
                        linked_mo_id_and_name[mo_id] for mo_id in mo_ids
                    ]
                    continue
                linked_mo_link_and_his_values[prm_id] = linked_mo_id_and_name[
                    int(mo_ids)
                ]

            for parameter_id, link in all_linked_prms.items():
                if isinstance(link, list):
                    all_linked_prms[parameter_id] = [
                        linked_mo_link_and_his_values.get(int(value), value)
                        for value in link
                    ]
                    continue
                all_linked_prms[parameter_id] = (
                    linked_mo_link_and_his_values.get(int(link), link)
                )

        else:
            prms_which_not_mo_link = []
            for parameter_id, linked_prms in all_linked_prms.items():
                if parameter_id not in linked_mo_ids:
                    if isinstance(linked_prms, list):
                        prms_which_not_mo_link.extend(linked_prms)
                        continue
                    prms_which_not_mo_link.append(linked_prms)

            prms_which_not_mo_link = [
                prms_which_not_mo_link[index : index + SQLALCHEMY_LIMIT]
                for index in range(
                    0, len(prms_which_not_mo_link), SQLALCHEMY_LIMIT
                )
            ]

            linked_params_with_values = {}
            for chunk in prms_which_not_mo_link:
                for parameter in (
                    session.execute(select(PRM).where(PRM.id.in_(chunk)))
                    .scalars()
                    .all()
                ):
                    if parameter.tprm.multiple:
                        linked_params_with_values[parameter.id] = (
                            utils.decode_multiple_value(parameter.value)
                        )
                        continue
                    linked_params_with_values[parameter.id] = parameter.value

            for parameter_id, linked_parameter_ids in all_linked_prms.items():
                if isinstance(linked_parameter_ids, list):
                    all_linked_prms[parameter_id] = [
                        linked_params_with_values.get(prm_id)
                        for prm_id in linked_parameter_ids
                    ]
                    continue
                all_linked_prms[parameter_id] = linked_params_with_values.get(
                    int(linked_parameter_ids)
                )

    # PROCESS NEW VALUES BY EARLIER GATHERED
    db_object = db_object.dict()
    db_object_id = db_object["id"]
    new_params = []

    for parameter in object_params:
        # data for mo_links and prm_links we gathered earlier
        if parameter.tprm.val_type == "mo_link":
            new_value = all_linked_mos[parameter.id]
        elif parameter.tprm.val_type == "prm_link":
            new_value = all_linked_prms[parameter.id]

        # so if its mot mo_link or prm_link we can check every other val_type by multiple\not multiple
        else:
            if parameter.tprm.multiple:
                new_value = utils.decode_multiple_value(parameter.value)
            else:
                new_value = parameter.value

        # FILL "PARAMS" ATTRIBUTE
        if with_parameters:
            new_params.append(
                PRMRead(
                    id=parameter.id,
                    tprm_id=parameter.tprm.id,
                    value=new_value,
                    mo_id=db_object_id,
                    version=parameter.version,
                )
            )

        if parameter.tprm.id in [
            object_type_instance.latitude,
            object_type_instance.longitude,
        ]:
            new_value = float(new_value)

        # ADD "RETURNABLE" PARAMS TO OBJECT ATTRIBUTE LEVEL
        if parameter.tprm.returnable:
            db_object[parameter.tprm.name] = new_value

    db_object["params"] = new_params
    return db_object


def get_parameters_for_object_by_object_query(
    session: Session,
    db_mos: list = None,
    mos_ids: list = None,
    returnable: bool = False,
    identifiers_instead_of_values: bool = False,
) -> list:
    mo_id_by_tmo_id = defaultdict(list)
    mos_by_id = {}
    for o in db_mos:
        mo_id_by_tmo_id[o.tmo_id].append(o.id)
        mos_by_id[o.id] = o
    if not mos_by_id:
        return []

    tprms_query = select(TPRM).where(TPRM.tmo_id.in_(mo_id_by_tmo_id))
    if returnable:
        tprms_query = tprms_query.where(TPRM.returnable == returnable)
    tprms: list[TPRM] = session.exec(tprms_query).all()
    tprms_by_id = {}
    tprm_ids_by_tmo_id = defaultdict(set)
    for tprm in tprms:
        tprms_by_id[tprm.id] = tprm
        tprm_ids_by_tmo_id[tprm.tmo_id].add(tprm.id)

    where_prms_ors = []
    for tmo_id, tprm_ids in tprm_ids_by_tmo_id.items():
        mos_ids_items = mo_id_by_tmo_id[tmo_id]
        where_tmo_tprm = and_(
            PRM.mo_id.in_(mos_ids_items), PRM.tprm_id.in_(tprm_ids)
        )
        where_prms_ors.append(where_tmo_tprm)

    prms_raw = []
    if where_prms_ors:
        prms_query = select(PRM).where(or_(*where_prms_ors))
        prms_raw = session.exec(prms_query).all()

    prms_by_mo_id = defaultdict(list)
    prms_by_id = {}
    other_prm_ids = []
    other_mo_ids = []
    links = []
    for prm_raw in prms_raw:
        prms_by_id[prm_raw.id] = prm_raw
        tprm = tprms_by_id[prm_raw.tprm_id]
        param_to_read = None
        if identifiers_instead_of_values and tprm.val_type in {
            "prm_link",
            "mo_link",
        }:
            tprm_dict = tprm.dict()
            tprm_dict["val_type"] = "int"
            tprm = TPRM(**tprm_dict)
        if tprm.multiple:
            match tprm.val_type:
                case "prm_link":
                    decoded_value = utils.decode_multiple_value(prm_raw.value)
                    other_prm_ids.extend(decoded_value)
                    links.append(prm_raw)
                case "mo_link":
                    decoded_value = utils.decode_multiple_value(prm_raw.value)
                    other_mo_ids.extend(decoded_value)
                    links.append(prm_raw)
                case _:
                    multiple_value = utils.decode_multiple_value(prm_raw.value)
                    param_to_read = PRMReadMultiple(
                        id=prm_raw.id,
                        tprm_id=prm_raw.tprm_id,
                        mo_id=prm_raw.mo_id,
                        value=multiple_value,
                        version=prm_raw.version,
                    )

        elif tprm.val_type == "prm_link":
            other_prm_ids.append(int(prm_raw.value))
            links.append(prm_raw)
        elif tprm.val_type == "mo_link":
            other_mo_ids.append(int(prm_raw.value))
            links.append(prm_raw)
        else:
            param_to_read = func.db_param_convert_by_val_type[
                prm_raw.tprm.val_type
            ](
                prm_raw.id,
                prm_raw.tprm_id,
                prm_raw.mo_id,
                prm_raw.value,
                prm_raw.version,
            )
        if param_to_read:
            prms_by_mo_id[prm_raw.mo_id].append(param_to_read)
    other_prm_ids = set(other_prm_ids).difference(set(prms_by_id))
    other_mo_ids = set(other_mo_ids).difference(set(mos_by_id))
    other_tprm_ids = []
    if other_prm_ids:
        other_prms_query = select(PRM).where(PRM.id.in_(other_prm_ids))
        other_prms = session.exec(other_prms_query).all()
        for other_prm in other_prms:
            prms_by_id[other_prm.id] = other_prm
            if other_prm.tprm_id not in tprms_by_id:
                other_tprm_ids.append(other_prm.tprm_id)
    if other_mo_ids:
        other_mos_query = select(MO).where(MO.id.in_(other_mo_ids))
        other_mos = session.exec(other_mos_query).all()
        for other_mo in other_mos:
            mos_by_id[other_mo.id] = other_mo
    other_tprm_ids = set(other_tprm_ids).difference(tprms_by_id)
    if other_tprm_ids:
        other_tprms_query = select(TPRM).where(TPRM.id.in_(other_tprm_ids))
        other_tprms = session.exec(other_tprms_query).all()
        for other_tprm in other_tprms:
            tprms_by_id[other_tprm.id] = other_tprm

    for link in links:
        tprm = tprms_by_id[link.tprm_id]
        if tprm.multiple:
            if tprm.val_type == "prm_link":
                decoded_value = utils.decode_multiple_value(link.value)
                multiple_value = []
                for v in decoded_value:
                    tprm_id = prms_by_id[v].tprm_id
                    single_val = func.value_convertation_by_val_type[
                        tprms_by_id[tprm_id].val_type
                    ](prms_by_id[v].value)
                    multiple_value.append(single_val)
            else:
                decoded_value = utils.decode_multiple_value(link.value)
                multiple_value = []
                for v in decoded_value:
                    multiple_value.append(mos_by_id[v].name)
            param_to_read = PRMReadMultiple(
                id=link.id,
                tprm_id=link.tprm_id,
                mo_id=link.mo_id,
                value=multiple_value,
                version=link.version,
            )
        elif tprm.val_type == "prm_link":
            tprm_id = prms_by_id[int(link.value)].tprm_id
            linked_param = prms_by_id[int(link.value)]
            param_to_read = func.db_param_convert_by_val_type[
                tprms_by_id[tprm_id].val_type
            ](
                link.id,
                link.tprm_id,
                link.mo_id,
                linked_param.value,
                link.version,
            )
        else:
            linked_mo = mos_by_id[int(link.value)]
            param_to_read = func.db_param_convert_by_val_type["str"](
                link.id, link.tprm_id, link.mo_id, linked_mo.name, link.version
            )
        prms_by_mo_id[link.mo_id].append(param_to_read)

    results = []
    for mo in db_mos:
        result = mo.dict()
        params = []
        for prm in prms_by_mo_id.get(mo.id, []):
            tprm = tprms_by_id[prm.tprm_id]
            if tprm.returnable:
                result[tprm.name] = prm.value
            params.append(prm.dict())
        result["params"] = params
        results.append(result)

    if mos_ids:
        results_by_id = {r["id"]: r for r in results}
        results = [results_by_id[m] for m in mos_ids if m in results_by_id]

    return results


def get_objects_with_parameters_by_mo_ids(
    session: Session,
    limit: int | None,
    offset: int | None,
    object_type_id: int = None,
    mos_ids: list = None,
    p_id: int = None,
    returnable: bool = False,
    active: bool = True,
    order_by_rule: OrderByRule = None,
    identifiers_instead_of_values: bool = False,
    with_parent_name: bool = False,
) -> list:
    if order_by_rule is not None:
        if order_by_rule == "desc":
            order_by_column = desc(MO.id)
        else:
            order_by_column = asc(MO.id)
    else:
        order_by_column = None

    mos_query = select(MO).where(
        MO.id.in_(mos_ids) if mos_ids is not None else True,
        MO.tmo_id == object_type_id if object_type_id is not None else True,
        MO.p_id == p_id if p_id is not None else True,
        MO.active == active,
    )
    if order_by_column is not None:
        mos_query = mos_query.order_by(order_by_column)
    if offset:
        mos_query = mos_query.offset(offset)
    if limit:
        mos_query = mos_query.limit(limit)

    db_mos = session.exec(mos_query).all()
    result = get_parameters_for_object_by_object_query(
        session=session,
        db_mos=db_mos,
        mos_ids=mos_ids,
        identifiers_instead_of_values=identifiers_instead_of_values,
        returnable=returnable,
    )
    if with_parent_name:
        result_object_index_with_p_ids = {}
        for result_object_index in range(len(result)):
            if result[result_object_index].get("p_id", None) is not None:
                result_object_index_with_p_ids[result_object_index] = result[
                    result_object_index
                ].get("p_id")

        real_p_id = [
            p_id
            for _, p_id in result_object_index_with_p_ids.items()
            if p_id is not None
        ]
        mos_query = select(MO.id, MO.name).where(MO.id.in_(set(real_p_id)))
        db_mos = session.exec(mos_query).mappings().all()

        p_id_and_name = {mo["id"]: mo["name"] for mo in db_mos}

        real_p_id = list(p_id_and_name.keys())
        for result_object_index in range(len(result)):
            if result[result_object_index]["p_id"] in real_p_id:
                parent_name = p_id_and_name[result[result_object_index]["p_id"]]
            else:
                parent_name = None

            result[result_object_index]["parent_name"] = parent_name
    return result


def get_objects_with_parameters(
    session: Session,
    limit: int | None,
    offset: int | None,
    object_type_id: int = None,
    mos_ids: list = None,
    p_id: int = None,
    returnable: bool = False,
    active: bool = True,
    order_by_rule: OrderByRule = None,
    identifiers_instead_of_values: bool = False,
    with_parent_name: bool = False,
) -> list:
    if order_by_rule is not None:
        if order_by_rule == "desc":
            order_by_column = desc(MO.id)
        else:
            order_by_column = asc(MO.id)
    else:
        order_by_column = MO.id

    mos_query = (
        select(MO)
        .where(
            MO.id.in_(mos_ids) if mos_ids is not None else True,
            MO.tmo_id == object_type_id if object_type_id is not None else True,
            MO.p_id == p_id if p_id is not None else True,
            MO.active == active,
        )
        .order_by(order_by_column)
        .offset(offset)
        .limit(limit)
    )
    if offset:
        mos_query.offset(offset)
    if limit:
        mos_query.limit(limit)
    db_mos = session.exec(mos_query).all()
    result = get_parameters_for_object_by_object_query(
        session=session,
        db_mos=db_mos,
        mos_ids=mos_ids,
        identifiers_instead_of_values=identifiers_instead_of_values,
        returnable=returnable,
    )
    if with_parent_name:
        result_object_index_with_p_ids = {}
        for result_object_index in range(len(result)):
            if result[result_object_index].get("p_id", None) is not None:
                result_object_index_with_p_ids[result_object_index] = result[
                    result_object_index
                ].get("p_id")

        real_p_id = [
            p_id
            for _, p_id in result_object_index_with_p_ids.items()
            if p_id is not None
        ]
        mos_query = select(MO.id, MO.name).where(MO.id.in_(set(real_p_id)))
        db_mos = session.exec(mos_query).mappings().all()

        p_id_and_name = {mo["id"]: mo["name"] for mo in db_mos}

        real_p_id = list(p_id_and_name.keys())
        for result_object_index in range(len(result)):
            if result[result_object_index]["p_id"] in real_p_id:
                parent_name = p_id_and_name[result[result_object_index]["p_id"]]
            else:
                parent_name = None

            result[result_object_index]["parent_name"] = parent_name
    return result


def get_filter_data_by_query_params(query_params: dict) -> list:
    filter_data = []
    regex_contains = re.compile(r"tprm_id(\d+)\.\*")
    regex_equal = re.compile(r"tprm_id(\d+)")
    for key, value in query_params.items():
        match = regex_contains.fullmatch(key)
        if match:
            tprm_id = int(regex_contains.findall(key)[0])
            values = value.split(";")
            filter_data.append(
                {"id": tprm_id, "operator": "contains", "values": values}
            )
        else:
            match = regex_equal.fullmatch(key)
            if match:
                tprm_id = int(regex_equal.findall(key)[0])
                values = value.split(";")
                filter_data.append(
                    {"id": tprm_id, "operator": "equal", "values": values}
                )
    return filter_data


def get_all_db_param_type_by_tmo_id_or_exception(
    session: Session, tmo_id: int, check_for_exists: bool = False
) -> List[TPRM]:
    stmt = select(TPRM).where(TPRM.tmo_id == tmo_id)
    db_param_type = session.exec(stmt).all()
    if check_for_exists and not db_param_type:
        raise HTTPException(
            status_code=422, detail=f"Parameter type doesn't exist in {tmo_id}."
        )
    return db_param_type


def get_db_param_type_or_exception(session: Session, tprm_id: int) -> TPRM:
    db_param_type = session.get(TPRM, tprm_id)
    if not db_param_type:
        raise HTTPException(
            status_code=404,
            detail=f"Parameter type with id {tprm_id} not found.",
        )
    return db_param_type


def get_db_param_type_and_prms_or_exception(
    session: Session, tprm_id: int
) -> TPRM:
    db_param_type = session.get(
        TPRM,
        tprm_id,
        options=[selectinload(TPRM.prms)],
    )
    if db_param_type:
        return db_param_type
    raise HTTPException(
        status_code=404,
        detail=f"Parameter type with id {tprm_id} not found.",
    )


def get_db_param_by_mo_and_tprm_or_exception(
    session: Session, mo_id: int, tprm_id: int
) -> PRM:
    results = session.exec(
        select(PRM).where(PRM.mo_id == mo_id, PRM.tprm_id == tprm_id)
    )
    param = results.first()
    if not param:
        raise HTTPException(
            status_code=404,
            detail=f"Parameter for mo id {mo_id} and tprm id {tprm_id} not found.",
        )
    return param


def get_params_to_read(db_params: list) -> list:
    params_list = []
    for db_param in db_params:
        params_list.append(
            get_param_to_read_based_on_multiple(db_param=db_param)
        )
    return params_list


def get_params_to_read_with_link_values(
    session: Session, db_params: List["PRM"]
) -> list:
    tprm_ids = set()
    prms_by_id = {p.id: p for p in db_params}
    for p in db_params:
        prms_by_id[p.id] = p
        tprm_ids.add(p.tprm_id)

    tprms_query = select(TPRM).where(TPRM.id.in_(tprm_ids))
    tprms_by_id = {t.id: t for t in session.exec(tprms_query).all()}

    other_prm_ids = []
    other_mo_ids = []
    links = []
    params_to_read = []
    for param in db_params:
        tprm = tprms_by_id[param.tprm_id]
        param_to_read = None
        if tprm.multiple:
            match tprm.val_type:
                case "prm_link":
                    decoded_value = utils.decode_multiple_value(param.value)
                    other_prm_ids.extend(decoded_value)
                    links.append(param)
                case "mo_link":
                    decoded_value = utils.decode_multiple_value(param.value)
                    other_mo_ids.extend(decoded_value)
                    links.append(param)
                case _:
                    multiple_value = utils.decode_multiple_value(param.value)
                    param_to_read = PRMReadMultiple(
                        id=param.id,
                        tprm_id=param.tprm_id,
                        mo_id=param.mo_id,
                        value=multiple_value,
                        version=param.version,
                    )
        elif tprm.val_type == "prm_link":
            other_prm_ids.append(int(param.value))
            links.append(param)
        elif tprm.val_type == "mo_link":
            other_mo_ids.append(int(param.value))
            links.append(param)
        else:
            param_to_read = func.db_param_convert_by_val_type[
                param.tprm.val_type
            ](param.id, param.tprm_id, param.mo_id, param.value, param.version)
        if param_to_read:
            params_to_read.append(param_to_read)
    other_prm_ids = set(other_prm_ids).difference(set(prms_by_id))
    other_mo_ids = set(other_mo_ids)  # пока без сравнения со списком МО
    other_tprm_ids = []
    if other_prm_ids:
        other_prms_query = select(PRM).where(PRM.id.in_(other_prm_ids))
        other_prms = session.exec(other_prms_query).all()
        for other_prm in other_prms:
            prms_by_id[other_prm.id] = other_prm
            if other_prm.tprm_id not in tprms_by_id:
                other_tprm_ids.append(other_prm.tprm_id)
    mos_by_id = {}
    if other_mo_ids:
        other_mos_query = select(MO).where(MO.id.in_(other_mo_ids))
        other_mos = session.exec(other_mos_query).all()
        for other_mo in other_mos:
            mos_by_id[other_mo.id] = other_mo
    other_tprm_ids = set(other_tprm_ids).difference(tprms_by_id)
    if other_tprm_ids:
        other_tprms_query = select(TPRM).where(TPRM.id.in_(other_tprm_ids))
        other_tprms = session.exec(other_tprms_query).all()
        for other_tprm in other_tprms:
            tprms_by_id[other_tprm.id] = other_tprm
    for link in links:
        tprm = tprms_by_id[link.tprm_id]
        if tprm.multiple:
            if tprm.val_type == "prm_link":
                decoded_value = utils.decode_multiple_value(link.value)
                multiple_value = []
                for v in decoded_value:
                    tprm_id = prms_by_id[v].tprm_id
                    single_val = func.value_convertation_by_val_type[
                        tprms_by_id[tprm_id].val_type
                    ](prms_by_id[v].value)
                    multiple_value.append(single_val)
            else:
                decoded_value = utils.decode_multiple_value(link.value)
                multiple_value = []
                for v in decoded_value:
                    multiple_value.append(mos_by_id[v].name)
            param_to_read = PRMReadMultiple(
                id=link.id,
                tprm_id=link.tprm_id,
                mo_id=link.mo_id,
                value=multiple_value,
                version=link.version,
            )
        elif tprm.val_type == "prm_link":
            tprm_id = prms_by_id[int(link.value)].tprm_id
            linked_param = prms_by_id[int(link.value)]
            param_to_read = func.db_param_convert_by_val_type[
                tprms_by_id[tprm_id].val_type
            ](
                link.id,
                link.tprm_id,
                link.mo_id,
                linked_param.value,
                link.version,
            )
        else:
            linked_mo = mos_by_id[int(link.value)]
            param_to_read = func.db_param_convert_by_val_type["str"](
                link.id, link.tprm_id, link.mo_id, linked_mo.name, link.version
            )
        params_to_read.append(param_to_read)

    return params_to_read


def get_unique_parameter_type_values_by_param_type(
    session: Session, param_type_id: id
) -> set:
    db_param_type = get_db_param_type_or_exception(
        session=session, tprm_id=param_type_id
    )
    mos = session.exec(
        select(MO).where(
            MO.tmo_id == db_param_type.tmo_id,
            MO.active == True,  # noqa
        )
    ).all()
    mos_ids = [mo.id for mo in mos]
    get_params_query = select(PRM).where(
        PRM.tprm_id == param_type_id, PRM.mo_id.in_(mos_ids)
    )
    if db_param_type.multiple:
        if db_param_type.val_type == "prm_link":
            prm_values = []

            link_prms = {
                p.id: p
                for p in session.exec(
                    select(PRM).where(
                        PRM.tprm_id == int(db_param_type.constraint)
                    )
                ).all()
            }

            prms = session.exec(get_params_query).all()
            for p in prms:
                prm_value = []
                decoded_value = utils.decode_multiple_value(p.value)
                for v in decoded_value:
                    prm_value.append(link_prms[v].value)
                prm_values.append(tuple(prm_value))
        elif db_param_type.val_type == "mo_link":
            prm_values = []

            link_mos = {
                m.id: m
                for m in session.exec(
                    select(MO).where(MO.tmo_id == int(db_param_type.constraint))
                ).all()
            }

            prms = session.exec(get_params_query).all()
            for p in prms:
                prm_value = []
                decoded_value = utils.decode_multiple_value(p.value)
                for v in decoded_value:
                    prm_value.append(link_mos[v].name)
                prm_values.append(tuple(prm_value))
        else:
            prm_values = [
                tuple(utils.decode_multiple_value(prm.value))
                for prm in session.exec(get_params_query).all()
            ]
    else:
        if db_param_type.val_type == "prm_link":
            prm_values = []

            link_prms = {
                p.id: p
                for p in session.exec(
                    select(PRM).where(
                        PRM.tprm_id == int(db_param_type.constraint)
                    )
                ).all()
            }

            link_tprm = session.get(TPRM, int(db_param_type.constraint))
            prms = session.exec(get_params_query).all()
            for p in prms:
                value = func.value_convertation_by_val_type[link_tprm.val_type](
                    link_prms[int(p.value)].value
                )
                prm_values.append(value)
        elif db_param_type.val_type == "mo_link":
            prm_values = []

            link_mos = {
                m.id: m
                for m in session.exec(
                    select(MO).where(MO.tmo_id == int(db_param_type.constraint))
                ).all()
            }

            prms = session.exec(get_params_query).all()
            for p in prms:
                value = func.value_convertation_by_val_type["str"](
                    link_mos[int(p.value)].name
                )
                prm_values.append(value)
        else:
            prm_values = [
                func.value_convertation_by_val_type[db_param_type.val_type](
                    prm.value
                )
                for prm in session.exec(get_params_query).all()
            ]
    unique_prm_values = set(prm_values)
    return unique_prm_values


def get_object_out_mo_links(session: Session, object_id: int) -> list:
    active_mos = session.exec(select(MO).where(MO.active == True)).all()  # noqa
    mos_ids = [str(mo.id) for mo in active_mos]
    single_out_links = session.exec(
        select(PRM)
        .join(TPRM)
        .where(
            PRM.value.in_(mos_ids),
            PRM.mo_id == object_id,
            TPRM.val_type == "mo_link",
            TPRM.multiple != True,  # noqa
        )
    ).all()
    single_out_links_to_read = get_params_to_read(db_params=single_out_links)
    multiple_out_links = session.exec(
        select(PRM)
        .join(TPRM)
        .where(
            PRM.mo_id == object_id,
            TPRM.val_type == "mo_link",
            TPRM.multiple == True,  # noqa
        )
    ).all()
    multiple_active_links = []
    for link in multiple_out_links:
        decoded_value = utils.decode_multiple_value(link.value)
        for single_link in decoded_value:
            if str(single_link) in mos_ids:
                multiple_active_links.append(link)
                break
    multiple_out_links_to_read = get_params_to_read(
        db_params=multiple_active_links
    )
    out_links_to_read = single_out_links_to_read + multiple_out_links_to_read
    return out_links_to_read


def get_object_in_mo_links(session, db_object):
    active_mos = session.exec(select(MO).where(MO.active == True)).all()  # noqa
    mos_ids = [mo.id for mo in active_mos]
    single_links = session.exec(
        select(PRM)
        .join(TPRM)
        .where(
            PRM.value == str(db_object.id),
            PRM.mo_id.in_(mos_ids),
            TPRM.val_type == "mo_link",
        )
    ).all()
    single_links_to_read = get_params_to_read(db_params=single_links)
    multiple_links_by_tmo_id = session.exec(
        select(PRM)
        .join(TPRM)
        .where(
            PRM.mo_id.in_(mos_ids),
            TPRM.constraint == str(db_object.object_type_id),
            TPRM.val_type == "mo_link",
            TPRM.multiple == True,  # noqa
        )
    ).all()
    multiple_links = []
    for link in multiple_links_by_tmo_id:
        if db_object.id in utils.decode_multiple_value(link.value):
            multiple_links.append(link)
    multiple_links_to_read = get_params_to_read(db_params=multiple_links)
    in_links_to_read = single_links_to_read + multiple_links_to_read
    return in_links_to_read


def get_route(session: Session, db_object: MO, route_list: list) -> list:
    if len(db_object.children) > 0:
        for child in db_object.children:
            get_route(session, child, route_list)
    else:
        if db_object.point_a is not None and db_object.point_b is not None:
            route_list.append(
                [
                    [db_object.point_a.latitude, db_object.point_a.longitude],
                    [db_object.point_b.latitude, db_object.point_b.longitude],
                ]
            )
    return route_list


def get_object_by_point_a_id_or_exception(
    session: Session, db_object_id: int
) -> MO:
    point_a_object = session.exec(
        select(MO).where(MO.id == db_object_id)
    ).first()
    if not point_a_object:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid point_a object id ({db_object_id}). Object "
            f"does not exist.",
        )
    return point_a_object


def get_object_by_point_b_id_or_exception(
    session: Session, db_object_id: int
) -> MO:
    point_b_object = session.exec(
        select(MO).where(MO.id == db_object_id)
    ).first()
    if not point_b_object:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid point_b object id ({db_object_id}). Object "
            f"does not exist.",
        )
    return point_b_object
