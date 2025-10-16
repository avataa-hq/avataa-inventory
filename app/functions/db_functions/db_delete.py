import json  # noqa
import json  # noqa
import pickle
from typing import List

from fastapi import HTTPException
from sqlalchemy import or_, cast, String, and_
from sqlmodel import select, Session

from common.common_utils import ValueTypeValidator
from database import SQLALCHEMY_LIMIT, get_chunked_values_by_sqlalchemy_limit
from functions.functions_utils import utils
from models import TMO, TPRM, MO, PRM
from routers.parameter_type_router.schemas import TPRMUpdate


def delete_mo_links_by_tmo_id(session: Session, object_type_id: int) -> None:
    session.info["disable_security"] = True
    already_deleted_param_ids = set()
    tprms_of_linked_params = set()

    # delete link by TPRMs where multiple is False
    object_ids_of_tmo = (
        session.execute(
            select(cast(MO.id, String)).where(MO.tmo_id == object_type_id)
        )
        .scalars()
        .all()
    )

    for index in range(0, len(object_ids_of_tmo), SQLALCHEMY_LIMIT):
        prm_ids_partition = object_ids_of_tmo[index : index + SQLALCHEMY_LIMIT]
        conditions = [
            TPRM.multiple.is_(False),
            TPRM.val_type == "mo_link",
            or_(
                TPRM.constraint.is_(None),
                TPRM.constraint == str(object_type_id),
            ),
        ]

        subquery = select(TPRM.id).where(*conditions)
        stmt = select(PRM).where(
            PRM.value.in_(prm_ids_partition), PRM.tprm_id.in_(subquery)
        )
        for param in session.execute(stmt).scalars().all():
            already_deleted_param_ids.add(str(param.id))
            tprms_of_linked_params.add(str(param.tprm_id))
            session.delete(param)

    # delete link by TPRMs where multiple is True
    object_ids_of_tmo = (
        session.execute(select(MO.id).where(MO.tmo_id == object_type_id))
        .scalars()
        .all()
    )

    subquery = select(TPRM.id).where(
        TPRM.val_type == "mo_link",
        TPRM.multiple.is_(True),
        or_(TPRM.constraint.is_(None), TPRM.constraint == str(object_type_id)),
    )
    params_to_delete = (
        session.execute(select(PRM).where(PRM.tprm_id.in_(subquery)))
        .scalars()
        .all()
    )

    for param in params_to_delete:
        param_value = utils.decode_multiple_value(param.value)
        if param_value:
            param.value = [
                mo_id for mo_id in param_value if mo_id not in object_ids_of_tmo
            ]
            if param.value:
                param.value = pickle.dumps(param.value).hex()
                session.add(param)
                continue

            param.value = pickle.dumps(param.value).hex()
            already_deleted_param_ids.add(str(param.id))
            tprms_of_linked_params.add(str(param.tprm_id))
            session.delete(param)

    # delete TPRMs, which linked to current TMO
    links_tprms = session.exec(
        select(TPRM).where(
            TPRM.val_type == "mo_link", TPRM.constraint == str(object_type_id)
        )
    ).all()

    for tprm in links_tprms:
        session.delete(tprm)

    for index in range(0, len(already_deleted_param_ids), SQLALCHEMY_LIMIT):
        prm_ids_partition = list(already_deleted_param_ids)[
            index : index + SQLALCHEMY_LIMIT
        ]
        conditions_for_subquery = [
            TPRM.val_type == "prm_link",
            TPRM.multiple.is_(False),
            or_(
                TPRM.constraint.is_(None),
                TPRM.constraint.in_(tprms_of_linked_params),
            ),
        ]
        subquery = select(TPRM.id).where(*conditions_for_subquery)

        conditions = [
            PRM.value.in_(prm_ids_partition),
            PRM.tprm_id.in_(subquery),
        ]
        for prm_link in (
            session.execute(select(PRM).where(*conditions)).scalars().all()
        ):
            session.delete(prm_link)

    subquery = select(TPRM.id).where(
        TPRM.val_type == "prm_link",
        TPRM.multiple.is_(True),
        or_(
            TPRM.constraint.is_(None),
            TPRM.constraint.in_(tprms_of_linked_params),
        ),
    )
    prm_links_to_delete = (
        session.execute(select(PRM).where(PRM.tprm_id.in_(subquery)))
        .scalars()
        .all()
    )
    for prm_link in prm_links_to_delete:
        multiple_value = utils.decode_multiple_value(prm_link.value)
        if multiple_value:
            multiple_value = [
                prm_link_id
                for prm_link_id in multiple_value
                if str(prm_link_id) not in already_deleted_param_ids
            ]
            if multiple_value:
                prm_link.value = pickle.dumps(multiple_value).hex()
                session.add(prm_link)
                continue

            session.delete(prm_link)


def delete_point_links_by_tmo_id(session, object_type_id):
    mos = session.exec(select(MO).where(MO.tmo_id == object_type_id)).all()
    mos_ids = [mo.id for mo in mos]
    line_mos = session.exec(
        select(MO).where(
            or_(MO.point_a_id.in_(mos_ids), MO.point_b_id.in_(mos_ids))
        )
    ).all()
    for line_mo in line_mos:
        line_mo.geometry = None
        session.add(line_mo)


def delete_prm_links_by_tmo_id(session: Session, object_type_id: int) -> None:
    session.info["disable_security"] = True
    param_types = session.exec(
        select(TPRM).where(TPRM.tmo_id == object_type_id)
    ).all()
    tprm_ids = []
    for param_type in param_types:
        tprm_ids.append(str(param_type.id))
    session.info["disable_security"] = True
    links_tprm = session.exec(
        select(TPRM).where(
            TPRM.val_type == "prm_link", TPRM.constraint.in_(tprm_ids)
        )
    ).all()
    for link_tprm in links_tprm:
        session.delete(link_tprm)


def delete_prm_links_by_tprm_id(session: Session, tprm_id: int) -> None:
    session.info["disable_security"] = True
    links_tprm = session.exec(
        select(TPRM).where(
            TPRM.val_type == "prm_link", TPRM.constraint == str(tprm_id)
        )
    ).all()
    for link_tprm in links_tprm:
        session.delete(link_tprm)


def delete_mo_links_by_mo_id(session: Session, mo: MO) -> None:
    session.info["disable_security"] = True
    limit = 30_000
    already_deleted_param_ids = set()
    links_to_already_deleted_prms = []
    tprms_of_linked_params = set()

    conditions_for_multiple = [
        TPRM.val_type == "mo_link",
        TPRM.multiple.is_(True),
        or_(TPRM.constraint.is_(None), TPRM.constraint == str(mo.tmo_id)),
    ]
    conditions_for_single = [
        TPRM.val_type == "mo_link",
        TPRM.multiple.is_(False),
        or_(TPRM.constraint.is_(None), TPRM.constraint == str(mo.tmo_id)),
    ]

    stmt_for_multiple_mo_link_tprm = (
        select(TPRM.id)
        .where(*conditions_for_multiple)
        .execution_options(yield_per=limit)
    )
    for mo_link_tprms_multiple in (
        session.execute(stmt_for_multiple_mo_link_tprm)
        .scalars()
        .partitions(size=limit)
    ):
        stmt = (
            select(PRM)
            .where(
                PRM.tprm_id.in_(mo_link_tprms_multiple), PRM.value.isnot(None)
            )
            .execution_options(yield_per=limit)
        )

        for partition in session.execute(stmt).scalars().partitions(size=limit):
            for mo_link_param in partition:
                multiple_value = utils.decode_multiple_value(
                    mo_link_param.value
                )
                if multiple_value:
                    multiple_value = [
                        linked_mo_id
                        for linked_mo_id in multiple_value
                        if linked_mo_id != mo.id
                    ]
                    if multiple_value:
                        mo_link_param.value = pickle.dumps(multiple_value).hex()
                        session.add(mo_link_param)
                        continue

                    already_deleted_param_ids.add(str(mo_link_param.id))
                    tprms_of_linked_params.add(str(mo_link_param.tprm_id))
                    session.delete(mo_link_param)

    stmt_for_single_mo_link_tprm = (
        select(TPRM.id)
        .where(*conditions_for_single)
        .execution_options(yield_per=limit)
    )
    for mo_link_tprms_single in (
        session.execute(stmt_for_single_mo_link_tprm)
        .scalars()
        .partitions(size=limit)
    ):
        stmt = (
            select(PRM)
            .where(
                PRM.tprm_id.in_(mo_link_tprms_single), PRM.value == str(mo.id)
            )
            .execution_options(yield_per=limit)
        )
        for partition in session.execute(stmt).scalars().partitions(size=limit):
            for mo_link_param in partition:
                already_deleted_param_ids.add(str(mo_link_param.id))
                tprms_of_linked_params.add(str(mo_link_param.tprm_id))
                session.delete(mo_link_param)

    for index in range(0, len(already_deleted_param_ids), SQLALCHEMY_LIMIT):
        prm_ids_chunk = list(already_deleted_param_ids)[
            index : index + SQLALCHEMY_LIMIT
        ]

        conditions = [
            TPRM.val_type == "prm_link",
            PRM.value.in_(prm_ids_chunk),
            TPRM.multiple.is_(False),
        ]
        links_to_already_deleted_prms.extend(
            session.execute(select(PRM).join(TPRM).where(*conditions))
            .scalars()
            .all()
        )

    for prm_link in links_to_already_deleted_prms:
        session.delete(prm_link)

    conditions = [
        TPRM.val_type == "prm_link",
        TPRM.multiple.is_(True),
        or_(
            TPRM.constraint.is_(None),
            TPRM.constraint.in_(tprms_of_linked_params),
        ),
    ]
    links_to_already_deleted_prms = (
        session.execute(select(PRM).join(TPRM).where(*conditions))
        .scalars()
        .all()
    )

    for prm_link in links_to_already_deleted_prms:
        multiple_value = utils.decode_multiple_value(prm_link.value)
        if multiple_value:
            multiple_value = [
                prm_link_id
                for prm_link_id in multiple_value
                if prm_link_id not in already_deleted_param_ids
            ]
            if multiple_value:
                prm_link.value = pickle.dumps(multiple_value).hex()
                session.add(prm_link)
                continue

            session.delete(prm_link)


def delete_mo_links_by_mo_id_list(
    session: Session, mo_list: List[MO] | set[MO], object_type_ids: List[str]
) -> None:
    tmo_ids = set()
    mo_ids = set()
    mo_ids_in_string_form = set()
    already_deleted_param_ids = set()
    links_to_already_deleted_prms = []
    tprms_of_linked_params = set()

    for mo in mo_list:
        tmo_ids.add(mo.tmo_id)
        mo_ids.add(mo.id)
        mo_ids_in_string_form.add(str(mo.id))

    if len(tmo_ids) != 1:
        raise HTTPException(
            status_code=422, detail="Objects must be from similar TMO"
        )

    session.info["disable_security"] = True

    # GATHER ALL LINKABLE TPRM`s
    conditions_for_multiple = [
        TPRM.val_type == "mo_link",
        TPRM.multiple.is_(True),
        or_(TPRM.constraint.is_(None), TPRM.constraint.in_(object_type_ids)),
    ]
    conditions_for_single = [
        TPRM.val_type == "mo_link",
        TPRM.multiple.is_(False),
        or_(TPRM.constraint.is_(None), TPRM.constraint.in_(object_type_ids)),
    ]

    stmt_for_multiple_mo_link_tprm = select(TPRM.id).where(
        *conditions_for_multiple
    )
    mo_link_tprms_multiple = (
        session.execute(stmt_for_multiple_mo_link_tprm).scalars().all()
    )

    # CHECK IF OBJECTS USES IN LINKS AND REMOVE LINKS
    # FOR MULTIPLE-LINK CASE
    for chunk in get_chunked_values_by_sqlalchemy_limit(mo_link_tprms_multiple):
        stmt = select(PRM).where(PRM.tprm_id.in_(chunk), PRM.value.isnot(None))

        for partition in get_chunked_values_by_sqlalchemy_limit(
            session.execute(stmt).scalars().all()
        ):
            for mo_link_param in partition:
                multiple_value = utils.decode_multiple_value(
                    mo_link_param.value
                )
                if multiple_value:
                    multiple_value = [
                        linked_mo_id
                        for linked_mo_id in multiple_value
                        if linked_mo_id not in mo_ids
                    ]
                    if multiple_value:
                        mo_link_param.value = pickle.dumps(multiple_value).hex()
                        session.add(mo_link_param)
                        continue

                    already_deleted_param_ids.add(str(mo_link_param.id))
                    tprms_of_linked_params.add(str(mo_link_param.tprm_id))
                    session.delete(mo_link_param)

    stmt_for_single_mo_link_tprm = select(TPRM.id).where(*conditions_for_single)
    mo_link_tprms_single = (
        session.execute(stmt_for_single_mo_link_tprm).scalars().all()
    )

    # FOR SINGLE-LINK CASE
    for chunk in get_chunked_values_by_sqlalchemy_limit(mo_link_tprms_single):
        stmt = select(PRM).where(
            PRM.tprm_id.in_(chunk), PRM.value.in_(mo_ids_in_string_form)
        )

        for partition in get_chunked_values_by_sqlalchemy_limit(
            session.execute(stmt).scalars().all()
        ):
            for mo_link_param in partition:
                already_deleted_param_ids.add(str(mo_link_param.id))
                tprms_of_linked_params.add(str(mo_link_param.tprm_id))
                session.delete(mo_link_param)

    for chunk in get_chunked_values_by_sqlalchemy_limit(
        already_deleted_param_ids
    ):
        conditions = [
            TPRM.val_type == "prm_link",
            PRM.value.in_(chunk),
            TPRM.multiple.is_(False),
        ]
        links_to_already_deleted_prms.extend(
            session.execute(select(PRM).join(TPRM).where(*conditions))
            .scalars()
            .all()
        )

    for prm_link in links_to_already_deleted_prms:
        session.delete(prm_link)

    conditions = [
        TPRM.val_type == "prm_link",
        TPRM.multiple.is_(True),
        or_(
            TPRM.constraint.is_(None),
            TPRM.constraint.in_(tprms_of_linked_params),
        ),
    ]
    links_to_already_deleted_prms = (
        session.execute(select(PRM).join(TPRM).where(*conditions))
        .scalars()
        .all()
    )

    for prm_link in links_to_already_deleted_prms:
        multiple_value = utils.decode_multiple_value(prm_link.value)
        if multiple_value:
            multiple_value = [
                prm_link_id
                for prm_link_id in multiple_value
                if prm_link_id not in already_deleted_param_ids
            ]
            if multiple_value:
                prm_link.value = pickle.dumps(multiple_value).hex()
                session.add(prm_link)
                continue

            session.delete(prm_link)


def delete_prm_links_by_mo_id(session: Session, mo_id: int) -> None:
    session.info["disable_security"] = True
    object_parameters = session.exec(
        select(PRM).where(PRM.mo_id == mo_id)
    ).all()
    object_prm_ids = {param.id for param in object_parameters}

    object_prm_ids_as_strs = [str(item_id) for item_id in object_prm_ids]

    session.info["disable_security"] = True
    prm_link_tprms = session.exec(
        select(TPRM).where(
            TPRM.val_type == "prm_link",
            TPRM.constraint.in_(object_prm_ids_as_strs),
        )
    ).all()
    for prm_link_tprm in prm_link_tprms:
        if prm_link_tprm.multiple:
            stmt = (
                select(PRM)
                .where(PRM.tprm_id == prm_link_tprm.id)
                .execution_options(yield_per=10000)
            )
            for partition in (
                session.execute(stmt).scalars().partitions(size=10000)
            ):
                for prm_link in partition:
                    multiple_value = utils.decode_multiple_value(prm_link.value)
                    if multiple_value:
                        new_mult_prm_value = []
                        prm_contains_deleted_prms_ids = False
                        for value in multiple_value:
                            if value in object_prm_ids:
                                prm_contains_deleted_prms_ids = True
                            else:
                                new_mult_prm_value.append(value)
                        if prm_contains_deleted_prms_ids:
                            if not new_mult_prm_value:
                                session.delete(prm_link)
                            else:
                                prm_link.value = pickle.dumps(
                                    new_mult_prm_value
                                ).hex()
                                session.add(prm_link)

        else:
            for prm_link in prm_link_tprm.prms:
                if int(prm_link.value) in object_prm_ids:
                    session.delete(prm_link)


def delete_prm_links_by_mo_id_list(
    session: Session, mo_ids: List[int] | set[int]
) -> None:
    session.info["disable_security"] = True
    object_prm_ids_as_strs = []
    for chunk in get_chunked_values_by_sqlalchemy_limit(mo_ids):
        object_parameters = (
            session.execute(
                select(cast(PRM.id, String)).where(PRM.mo_id.in_(chunk))
            )
            .scalars()
            .all()
        )
        object_prm_ids_as_strs.extend(object_parameters)

    object_prm_ids_as_strs = set(object_prm_ids_as_strs)

    session.info["disable_security"] = True

    prm_link_tprms = []
    for chunk in get_chunked_values_by_sqlalchemy_limit(object_prm_ids_as_strs):
        stmt = select(TPRM).where(
            TPRM.val_type == "prm_link", TPRM.constraint.in_(chunk)
        )
        prm_link_tprms.extend(session.exec(stmt).all())

    for prm_link_tprm in prm_link_tprms:
        if prm_link_tprm.multiple:
            stmt = select(PRM).where(PRM.tprm_id == prm_link_tprm.id)
            for partition in session.execute(stmt).scalars().all():
                for prm_link in partition:
                    multiple_value = utils.decode_multiple_value(prm_link.value)
                    if multiple_value:
                        new_mult_prm_value = []
                        prm_contains_deleted_prms_ids = False
                        for value in multiple_value:
                            if str(value) in object_prm_ids_as_strs:
                                prm_contains_deleted_prms_ids = True
                            else:
                                new_mult_prm_value.append(value)

                        if prm_contains_deleted_prms_ids:
                            if not new_mult_prm_value:
                                session.delete(prm_link)
                            else:
                                prm_link.value = pickle.dumps(
                                    new_mult_prm_value
                                ).hex()
                                session.add(prm_link)

        else:
            for prm_link in prm_link_tprm.prms:
                if prm_link.value in object_prm_ids_as_strs:
                    session.delete(prm_link)


def params_deleting_by_changing_constraint(
    session: Session, db_param_type: TPRM, param_type: TPRMUpdate
) -> None:
    session.info["disable_security"] = True
    db_params = session.exec(
        select(PRM).where(PRM.tprm_id == db_param_type.id)
    ).all()
    longitude = False
    latitude = False
    status = False
    session.info["disable_security"] = True
    db_tmo = session.get(TMO, db_param_type.tmo_id)
    if db_tmo.latitude == db_param_type.id:
        latitude = True
    if db_tmo.longitude == db_param_type.id:
        longitude = True
    if db_tmo.status == db_param_type.id:
        status = True
    for db_param in db_params:
        try:
            if db_param_type.multiple:
                value_to_validate = utils.decode_multiple_value(db_param.value)
            else:
                value_to_validate = db_param.value

            validation_task = ValueTypeValidator(
                session=session,
                parameter_type_instance=db_param_type,
                value_to_validate=value_to_validate,
            )
            validation_task.validate()

        except:  # noqa
            delete_prm_links_by_prm_id(
                session=session, parameter_instance=db_param
            )
            if longitude:
                db_mo = session.get(MO, db_param.mo_id)
                db_mo.longitude = None
                session.add(db_mo)

            if latitude:
                db_mo = session.get(MO, db_param.mo_id)
                db_mo.latitude = None
                session.add(db_mo)

            if status:
                db_mo = session.get(MO, db_param.mo_id)
                db_mo.status = None
                session.add(db_mo)

            session.delete(db_param)


def delete_prm_links_by_prm_id(
    session: Session, parameter_instance: PRM
) -> None:
    limit = 10_000

    session.info["disable_security"] = True
    query = select(TPRM).where(
        and_(
            TPRM.val_type == "prm_link",
            or_(
                TPRM.constraint.is_(None),
                TPRM.constraint == str(parameter_instance.tprm_id),
            ),
        )
    )

    prm_link_tprms = session.exec(query).all()

    multiple_tprm_ids = [
        tprm.id
        for tprm in prm_link_tprms
        if tprm.multiple == True  # noqa
    ]
    single_tprm_ids = [
        tprm.id
        for tprm in prm_link_tprms
        if tprm.multiple == False  # noqa
    ]

    session.info["disable_security"] = True
    stmt = (
        select(PRM)
        .where(
            PRM.tprm_id.in_(single_tprm_ids),
            PRM.value == str(parameter_instance.id),
        )
        .execution_options(yield_per=limit)
    )

    for links in session.execute(stmt).scalars().partitions(size=limit):
        for link in links:
            session.delete(link)

    stmt = (
        select(PRM)
        .where(PRM.tprm_id.in_(multiple_tprm_ids))
        .execution_options(yield_per=limit)
    )
    for links in session.execute(stmt).scalars().partitions(size=limit):
        for link in links:
            multiple_value = utils.decode_multiple_value(link.value)
            if parameter_instance.id in multiple_value:
                if len(multiple_value) == 1:
                    session.delete(link)
                    continue

                multiple_value.remove(parameter_instance.id)
                link.value = pickle.dumps(multiple_value).hex()
                session.add(link)

        session.flush()
