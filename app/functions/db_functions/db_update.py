import time
from datetime import datetime
from typing import List

from fastapi import HTTPException
from sqlalchemy import and_, or_, JSON, cast, literal, String, text
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import Session, select

from common.common_constant import NAME_DELIMITER
from functions.db_functions.db_read import (
    get_db_object_type_or_exception,
    get_db_param_type_or_exception,
)
from functions.functions_utils.utils import (
    update_object_type_attribute,
    set_location_attrs,
    extract_location_data,
    find_deep_parent,
)
from models import TMO, MO, TPRM, PRM
from routers.object_router.schemas import MOUpdate
from routers.object_router.utils import (
    update_child_location_mo,
    update_geometry,
    get_value_for_sequence,
)
from routers.object_type_router.schemas import TMOUpdate
from routers.object_type_router.utils import (
    new_and_old_object_types_equal,
    validate_lifecycle_process_definition,
    set_names_for_objects_on_tmo_update,
    set_labels_for_objects_on_tmo_update,
)
from val_types.constants import enum_val_type_name


def update_db_object_type(
    session: Session, db_object_type: TMO, object_type: TMOUpdate
) -> None:
    object_type_data = object_type.dict(exclude_unset=True)

    if new_and_old_object_types_equal(
        new_object_type_data=object_type_data,
        old_object_type_data=db_object_type,
    ):
        return db_object_type

    if db_object_type.version != object_type.version:
        raise HTTPException(
            status_code=409,
            detail=f"Actual version of TMO: {db_object_type.version}.",
        )
    if (
        object_type.lifecycle_process_definition is not None
        and object_type.lifecycle_process_definition != ""
    ):
        validate_lifecycle_process_definition(
            object_type.lifecycle_process_definition
        )
    if (
        "severity_id" in object_type_data
        and object_type_data["severity_id"] is not None
    ):
        tprm = get_db_param_type_or_exception(
            session=session, tprm_id=object_type_data["severity_id"]
        )
        if tprm.tmo_id != db_object_type.id:
            raise HTTPException(
                status_code=409,
                detail="Severity id it's TPRM id, which need to be from current TMO.",
            )

    # 'points_constraint_by_tmo' stores list of tmo ids, and feature point_a\b have to be in these TMO`s
    if "points_constraint_by_tmo" in object_type_data:
        exists_tmos = (
            session.execute(
                select(TMO.id).where(
                    TMO.id.in_(object_type.points_constraint_by_tmo)
                )
            )
            .scalars()
            .all()
        )
        requested_tmos_as_constraint = set(object_type.points_constraint_by_tmo)

        # if there TMO`s which are not exists -- we need to raise error
        differance = requested_tmos_as_constraint.difference(set(exists_tmos))
        if differance:
            raise HTTPException(
                status_code=422,
                detail="There are TMO`s, which can't be used by constraint, because of they are not "
                f"exists: {differance}",
            )

    if "primary" in object_type_data:
        if object_type.primary is None:
            raise HTTPException(
                status_code=422, detail="Primary field should be list."
            )
        elif len(object_type.primary) == 0:
            objects = session.exec(
                select(MO).where(MO.tmo_id == db_object_type.id)
            ).all()
            for obj in objects:
                obj.name = str(obj.id)
                session.add(obj)
        else:
            for tprm_id in object_type.primary:
                is_valid = session.exec(
                    select(TPRM).where(
                        TPRM.id == tprm_id,
                        TPRM.required == True,  # noqa
                        TPRM.multiple != True,  # noqa
                        TPRM.val_type.in_(
                            ["str", "int", "float", "mo_link", "formula"]
                        ),
                        TPRM.tmo_id == db_object_type.id,
                    )
                ).first()
                if not is_valid:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Invalid param type id in primary field: {tprm_id}.",
                    )
            set_names_for_objects_on_tmo_update(
                session=session,
                old_object_type=db_object_type,
                new_primary=object_type.primary,
            )

    if "label" in object_type_data:
        if object_type.label is None:
            raise HTTPException(
                status_code=422, detail="Label field should be list."
            )
        for tprm_id in object_type.label:
            is_valid = session.exec(
                select(TPRM).where(
                    TPRM.id == tprm_id,
                    TPRM.required == True,  # noqa
                    TPRM.multiple != True,  # noqa
                    TPRM.val_type.in_(
                        ["str", "int", "float", "mo_link", "formula"]
                    ),
                    TPRM.tmo_id == db_object_type.id,
                )
            ).first()
            if not is_valid:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid param type id in label field: {tprm_id}.",
                )
        set_labels_for_objects_on_tmo_update(
            session=session,
            object_type_instance=db_object_type,
            new_label=object_type.label,
        )

    if "name" in object_type_data and object_type_data["name"] is not None:
        if session.exec(
            select(TMO).where(
                TMO.name == object_type_data["name"],
                TMO.id != db_object_type.id,
            )
        ).first():
            raise HTTPException(
                status_code=422,
                detail=f"Object type parameter already exists with name: {object_type_data['name']}",
            )

    if "p_id" in object_type_data and object_type_data["p_id"] is not None:
        if object_type_data["p_id"] == db_object_type.id or not session.get(
            TMO, object_type_data["p_id"]
        ):
            raise HTTPException(status_code=422, detail="Invalid parent id.")

    if (
        "latitude" in object_type_data
        and object_type_data["latitude"] is not None
    ):
        lat_param_type = session.exec(
            select(TPRM).where(
                TPRM.id == object_type_data["latitude"],
                TPRM.tmo_id == db_object_type.id,
            )
        ).first()
        if not lat_param_type:
            raise HTTPException(
                status_code=422,
                detail="Invalid latitude value. Please, pass correct param type id.",
            )
        if lat_param_type.val_type != "float":
            raise HTTPException(
                status_code=422, detail="Latitude param type should be float."
            )
        if lat_param_type.multiple:
            raise HTTPException(
                status_code=422,
                detail="Latitude param type should be not multiple.",
            )

        latitude_prms = session.exec(
            select(PRM).where(PRM.tprm_id == lat_param_type.id)
        ).all()
        updated_mo_ids = []
        for prm in latitude_prms:
            db_object = session.get(MO, prm.mo_id)
            db_object.latitude = float(prm.value)
            updated_mo_ids.append(prm.mo_id)
            session.add(db_object)
        if db_object_type.latitude is not None:
            prev_lat_prms = session.exec(
                select(PRM).where(
                    PRM.tprm_id == db_object_type.latitude,
                    PRM.mo_id.notin_(updated_mo_ids),
                )
            ).all()
            for prm in prev_lat_prms:
                db_object = session.get(MO, prm.mo_id)
                db_object.latitude = None
                session.add(db_object)
        children_tmo = session.exec(
            select(TMO).where(TMO.p_id == db_object_type.id)
        ).all()
        for child_tmo in children_tmo:
            if child_tmo.inherit_location:
                all_parent_mo = session.exec(
                    select(MO).where(MO.tmo_id == db_object_type.id)
                ).all()
                for parent_mo in all_parent_mo:
                    child_mo = session.exec(
                        select(MO).where(
                            MO.tmo_id == child_tmo.id, MO.p_id == parent_mo.id
                        )
                    ).all()
                    location_data = extract_location_data(
                        geometry_type=db_object_type.geometry_type,
                        parent_mo=parent_mo,
                    )
                    set_location_attrs(
                        session=session,
                        db_param=db_object_type.geometry_type,
                        child_mos=child_mo,
                        set_value=True,
                        location_data=location_data,
                    )

    if (
        "longitude" in object_type_data
        and object_type_data["longitude"] is not None
    ):
        long_param_type = session.exec(
            select(TPRM).where(
                TPRM.id == object_type_data["longitude"],
                TPRM.tmo_id == db_object_type.id,
            )
        ).first()
        if not long_param_type:
            raise HTTPException(
                status_code=422,
                detail="Invalid longitude value. Please, pass correct param type id.",
            )
        if long_param_type.val_type != "float":
            raise HTTPException(
                status_code=422, detail="Longitude param type should be float."
            )
        if long_param_type.multiple:
            raise HTTPException(
                status_code=422,
                detail="Longitude param type should be not multiple.",
            )

        longitude_prms = session.exec(
            select(PRM).where(PRM.tprm_id == long_param_type.id)
        ).all()
        updated_mo_ids = []
        for prm in longitude_prms:
            db_object = session.get(MO, prm.mo_id)
            db_object.longitude = float(prm.value)
            updated_mo_ids.append(prm.mo_id)
            session.add(db_object)
        if db_object_type.longitude is not None:
            prev_long_prms = session.exec(
                select(PRM).where(
                    PRM.tprm_id == db_object_type.longitude,
                    PRM.mo_id.notin_(updated_mo_ids),
                )
            ).all()
            for prm in prev_long_prms:
                db_object = session.get(MO, prm.mo_id)
                db_object.longitude = None
                session.add(db_object)
        children_tmo = session.exec(
            select(TMO).where(TMO.p_id == db_object_type.id)
        ).all()
        for child_tmo in children_tmo:
            if child_tmo.inherit_location:
                all_parent_mo = session.exec(
                    select(MO).where(MO.tmo_id == db_object_type.id)
                ).all()
                for parent_mo in all_parent_mo:
                    child_mo = session.exec(
                        select(MO).where(
                            MO.tmo_id == child_tmo.id, MO.p_id == parent_mo.id
                        )
                    ).all()
                    location_data = extract_location_data(
                        geometry_type=db_object_type.geometry_type,
                        parent_mo=parent_mo,
                    )
                    set_location_attrs(
                        session=session,
                        db_param=db_object_type.geometry_type,
                        child_mos=child_mo,
                        set_value=True,
                        location_data=location_data,
                    )

    if "status" in object_type_data and object_type_data["status"] is not None:
        status_param_type = session.exec(
            select(TPRM).where(
                TPRM.id == object_type_data["status"],
                TPRM.tmo_id == db_object_type.id,
            )
        ).first()
        if not status_param_type:
            raise HTTPException(
                status_code=422,
                detail="Invalid status value. Please, pass correct param type id.",
            )
        if status_param_type.val_type not in [
            "str",
            "int",
            "float",
            enum_val_type_name,
        ]:
            raise HTTPException(
                status_code=422,
                detail="Status param type should be str, int or float.",
            )
        if status_param_type.multiple:
            raise HTTPException(
                status_code=422,
                detail="Status param type should be not multiple.",
            )

        status_prms = session.exec(
            select(PRM).where(PRM.tprm_id == status_param_type.id)
        ).all()
        updated_mo_ids = []
        for prm in status_prms:
            db_object = session.get(MO, prm.mo_id)
            db_object.status = prm.value
            updated_mo_ids.append(prm.mo_id)
            session.add(db_object)
        if db_object_type.status is not None:
            prev_status_prms = session.exec(
                select(PRM).where(
                    PRM.tprm_id == db_object_type.status,
                    PRM.mo_id.notin_(updated_mo_ids),
                )
            ).all()
            for prm in prev_status_prms:
                db_object = session.get(MO, prm.mo_id)
                db_object.status = None
                session.add(db_object)

    if "inherit_location" in object_type_data:
        if not db_object_type.geometry_type:
            raise HTTPException(
                status_code=422,
                detail="Unable to set inherit location for object type without geometry type.",
            )
        if not db_object_type.p_id:
            raise HTTPException(
                status_code=422,
                detail="Unable to set inherit location for object type without parent.",
            )
        start_time = time.perf_counter()
        all_mo = session.execute(
            select(MO).where(MO.tmo_id == db_object_type.id)
        ).scalars()
        for current_mo in all_mo:  # type: MO
            if not current_mo.p_id:
                print(
                    f"Couldn't set inherit location for {current_mo.id=}. Incorrect parent for mo."
                )
                continue
            if object_type_data["inherit_location"]:
                deep_parent_mo: MO = find_deep_parent(
                    session=session,
                    object_instance=current_mo,
                    object_type_instance=db_object_type,
                    from_parent=True,
                )[1]
                if not deep_parent_mo:
                    print(
                        f"Couldn't set inherit location for {current_mo.id=}. Incorrect parent."
                    )
                    continue
                location_data = extract_location_data(
                    geometry_type=db_object_type.geometry_type,
                    parent_mo=deep_parent_mo,
                )
                set_location_attrs(
                    session=session,
                    db_param=db_object_type.geometry_type,
                    child_mos=[current_mo],
                    set_value=True,
                    location_data=location_data,
                )
            else:
                set_location_attrs(
                    session=session,
                    db_param=db_object_type.geometry_type,
                    child_mos=[current_mo],
                )
        end_time = time.perf_counter()
        print(f"Full time: {end_time - start_time} {start_time=} {end_time=}")

    if "geometry_type" in object_type_data:
        # Update information for mo if line updated
        if (
            db_object_type.geometry_type
            and db_object_type.geometry_type == "line"
        ):
            stmt = text(
                f"""
            SELECT mo.*
              FROM mo, json_each(mo.geometry)
             WHERE mo.geometry::varchar != 'null' AND
                   mo.point_a_id IS NOT NULL AND
                   mo.point_b_id IS NOT NULL AND
                   tmo_id = {db_object_type.id} AND
                   key = 'path' AND
                   json_typeof(value) != 'string'"""
            )
            incorrect_mo = (
                session.exec(select(MO).from_statement(stmt)).scalars().all()
            )
            incorrect_mo.extend(
                session.exec(
                    select(MO).where(
                        and_(
                            MO.tmo_id == db_object_type.id,
                            MO.point_a_id.is_not(None),
                            MO.point_b_id.is_not(None),
                            cast(MO.geometry, String)
                            == cast(literal(JSON.NULL, JSON()), String),
                        )
                    )
                ).all()
            )
            for mo in incorrect_mo:
                mo.geometry = update_geometry(
                    object_instance=mo, point_a=mo.point_a, point_b=mo.point_b
                )
                mo.version += 1
                session.add(mo)
            session.commit()

    object_type_data["version"] += 1

    for key, value in object_type_data.items():
        setattr(db_object_type, key, value)
    db_object_type.modification_date = datetime.utcnow()


def update_db_object(session: Session, db_object: MO, object: MOUpdate) -> None:
    object_data = object.dict(exclude_unset=True)
    object_data["version"] += 1
    session.info["disable_security"] = True
    object_type = session.get(TMO, db_object.tmo_id)
    if "active" in object_data and (
        not object_data["active"] or object_data["active"] is None
    ):
        raise HTTPException(
            status_code=422,
            detail="Unable to archive object in this way. Use DELETE method.",
        )
    if "tmo_id" in object_data and object_data["tmo_id"] is not None:
        if not session.get(TMO, object.tmo_id):
            raise HTTPException(
                status_code=422, detail=f"Invalid tmo_id ({object.tmo_id})."
            )
    if "p_id" in object_data:
        if object_data["p_id"] is not None:
            parent_object = session.exec(
                select(MO).where(MO.id == object_data["p_id"])
            ).first()
            if not parent_object:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid parent id ({object_data['p_id']}). Object does not exist.",
                )
            if object_type.p_id is None:
                raise HTTPException(
                    status_code=422,
                    detail=f"Its impossible to create parent link, because TMO "
                    f"with id {object_type.id} has no parent",
                )
            if object_type.p_id != parent_object.tmo_id:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid parent id ({object_data['p_id']})."
                    f" Parent should be object of object type"
                    f" with id {object_type.p_id}.",
                )
            if len(object_type.primary) > 0:
                primary_values = []
                if not object_type.global_uniqueness:
                    primary_values.append(parent_object.name)
                for tprm_id in object_type.primary:
                    for prm in db_object.prms:
                        if prm.tprm_id == tprm_id:
                            tprm_val_type = session.exec(
                                select(TPRM.val_type).where(TPRM.id == tprm_id)
                            ).first()

                            if tprm_val_type == "mo_link":
                                prm_value = session.exec(
                                    select(MO.name).where(
                                        MO.id == int(prm.value)
                                    )
                                ).first()
                            else:
                                prm_value = str(prm.value)

                            primary_values.append(prm_value)
                name = NAME_DELIMITER.join(primary_values)
                session.info["disable_security"] = True
                name_exist = session.exec(
                    select(MO).where(
                        MO.id != db_object.id,
                        MO.name == name,
                        MO.tmo_id == object_type.id,
                    )
                ).first()
                if name_exist:
                    raise HTTPException(
                        status_code=422,
                        detail="Unable to set this parent. Object names will be not unique.",
                    )
                object_data["name"] = name
            if db_object.tmo.inherit_location:
                location_data = extract_location_data(
                    geometry_type=db_object.tmo.geometry_type,
                    parent_mo=parent_object,
                )
                set_location_attrs(
                    session=session,
                    db_param=db_object.tmo.geometry_type,
                    child_mos=[db_object],
                    set_value=True,
                    location_data=location_data,
                )
        else:
            if len(object_type.primary) > 0:
                primary_values = []
                for tprm_id in object_type.primary:
                    for prm in db_object.prms:
                        if prm.tprm_id == tprm_id:
                            primary_values.append(str(prm.value))
                name = NAME_DELIMITER.join(primary_values)
                name_exist = session.exec(
                    select(MO).where(
                        MO.id != db_object.id,
                        MO.name == name,
                        MO.tmo_id == object_type.id,
                    )
                ).first()
                if name_exist:
                    raise HTTPException(
                        status_code=422,
                        detail="Unable to set p_id to null. Object names will be not unique.",
                    )
        if db_object.tmo.inherit_location:
            set_location_attrs(
                session=session,
                db_param=db_object.tmo.geometry_type,
                child_mos=[db_object],
                set_value=True,
                location_data={},
            )
    if (
        "point_a_id" in object_data
        and "point_b_id" in object_data
        and object_type.geometry_type == "line"
    ):
        # Update geometry
        conditions = [MO.id == object_data["point_a_id"]]
        if object_type.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type.points_constraint_by_tmo)
            )
        new_point_a: MO | None = session.execute(
            select(MO).where(*conditions)
        ).scalar_one_or_none()
        conditions = [MO.id == object_data["point_b_id"]]
        if object_type.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type.points_constraint_by_tmo)
            )
        new_point_b: MO | None = session.execute(
            select(MO).where(*conditions)
        ).scalar_one_or_none()
        db_object.geometry = update_geometry(
            object_instance=db_object, point_a=new_point_a, point_b=new_point_b
        )
        flag_modified(db_object, "geometry")
        # Update child inherit location
        data = {
            "point_a_id": new_point_a.id if new_point_a else None,
            "point_b_id": new_point_b.id if new_point_b else None,
            "geometry": db_object.geometry,
        }
        update_child_location_mo(
            session=session,
            object_instance=db_object,
            object_type_instance=object_type,
            new_data=data,
        )
    elif "point_a_id" in object_data and object_type.geometry_type == "line":
        # Update geometry
        conditions = [MO.id == object_data["point_a_id"]]
        if object_type.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type.points_constraint_by_tmo)
            )
        new_point_a: MO | None = session.execute(
            select(MO).where(*conditions)
        ).scalar_one_or_none()
        if new_point_a and db_object.point_b_id:
            db_object.geometry = update_geometry(
                object_instance=db_object,
                point_a=new_point_a,
                point_b=db_object.point_b,
            )
            flag_modified(db_object, "geometry")
        # Update child inherit location
        data = {
            "point_a_id": new_point_a.id if new_point_a else None,
            "geometry": db_object.geometry,
        }
        update_child_location_mo(
            session=session,
            object_instance=db_object,
            object_type_instance=object_type,
            new_data=data,
        )
    elif "point_b_id" in object_data and object_type.geometry_type == "line":
        # Update geometry
        conditions = [MO.id == object_data["point_b_id"]]
        if object_type.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type.points_constraint_by_tmo)
            )
        new_point_b: MO | None = session.execute(
            select(MO).where(*conditions)
        ).scalar_one_or_none()
        if new_point_b and db_object.point_a_id:
            db_object.geometry = update_geometry(
                object_instance=db_object,
                point_a=db_object.point_a,
                point_b=new_point_b,
            )
            flag_modified(db_object, "geometry")
        # Update child inherit location
        data = {
            "point_b_id": new_point_b.id if new_point_b else None,
            "geometry": db_object.geometry,
        }
        update_child_location_mo(
            session=session,
            object_instance=db_object,
            object_type_instance=object_type,
            new_data=data,
        )
    if "geometry" in object_data:
        update_child_location_mo(
            session=session,
            object_instance=db_object,
            object_type_instance=object_type,
            new_data={"geometry": object_data["geometry"]},
        )

    if object_type.label:
        label_values = []
        for tprm_id in object_type.label:
            for prm in db_object.prms:
                if prm.tprm_id == tprm_id:
                    label_values.append(str(prm.value))
        name = NAME_DELIMITER.join(label_values)
        object_data["label"] = name
    else:
        object_data["label"] = None

    # initialize new value for sequence if object unarchived
    if object_data.get("active") and not db_object.active:
        query = select(TPRM).where(
            TPRM.tmo_id == db_object.tmo_id, TPRM.val_type == "sequence"
        )
        sequence_tprms = session.execute(query)
        sequence_tprms = sequence_tprms.scalars().all()
        for sequence_tprm in sequence_tprms:
            seq_type = None
            if sequence_tprm.constraint:
                query = select(PRM.value).where(
                    PRM.mo_id == db_object.id,
                    PRM.tprm_id == int(sequence_tprm.constraint),
                )
                seq_type = session.execute(query)
                seq_type = seq_type.scalar()

            seq_value = get_value_for_sequence(session, sequence_tprm, seq_type)

            # check if sequence already exists
            stmt = select(PRM).where(
                PRM.tprm_id == sequence_tprm.id, PRM.mo_id == db_object.id
            )
            sequence_exists = session.execute(stmt).scalar()
            if sequence_exists:
                sequence_exists.value = seq_value
                sequence_exists.version = 1
            else:
                sequence_exists = PRM(
                    tprm_id=sequence_tprm.id,
                    value=seq_value,
                    mo_id=db_object.id,
                )

            session.add(sequence_exists)

    for key, value in object_data.items():
        setattr(db_object, key, value)


def update_object_type_if_longitude_or_latitude_or_status(
    session: Session, param_type: TPRM
) -> None:
    session.info["disable_security"] = True
    db_object_type = get_db_object_type_or_exception(
        session=session, object_type_id=param_type.tmo_id
    )

    if db_object_type.latitude == param_type.id:
        update_object_type_attribute(
            session=session,
            param_type=param_type,
            db_object_type=db_object_type,
            attribute="latitude",
        )

    if db_object_type.longitude == param_type.id:
        update_object_type_attribute(
            session=session,
            param_type=param_type,
            db_object_type=db_object_type,
            attribute="longitude",
        )

    if db_object_type.status == param_type.id:
        update_object_type_attribute(
            session=session,
            param_type=param_type,
            db_object_type=db_object_type,
            attribute="status",
        )


def update_prm_links_by_tprm_id(
    session: Session, current_tprm: TPRM, linked_tprms: List["TPRM"]
):
    session.info["disable_security"] = True
    for link_tprm in linked_tprms:
        # updated_tprm_id = link_tprm.id
        link_tprm.val_type = current_tprm.val_type
        link_tprm.constraint = current_tprm.constraint
        link_tprm.version = link_tprm.version + 1
        session.add(link_tprm)
        session.commit()


def update_object_type_if_delete_longitude_or_latitude_or_status(
    session, param_type
):
    session.info["disable_security"] = True
    db_object_type = session.get(TMO, param_type.tmo_id)

    if db_object_type.latitude == param_type.id:
        db_object_type.latitude = None
        session.add(db_object_type)
        session.info["disable_security"] = True
        params = session.exec(
            select(PRM).where(PRM.tprm_id == param_type.id)
        ).all()
        for prm in params:
            object = session.get(MO, prm.mo_id)
            object.latitude = None
            session.add(object)
            line_mos = session.exec(
                select(MO).where(
                    or_(MO.point_a_id == object.id, MO.point_b_id == object.id)
                )
            ).all()
            for line_mo in line_mos:
                line_mo.geometry = None
                session.add(line_mo)
    if db_object_type.longitude == param_type.id:
        db_object_type.longitude = None
        session.add(db_object_type)
        session.info["disable_security"] = True
        params = session.exec(
            select(PRM).where(PRM.tprm_id == param_type.id)
        ).all()
        for prm in params:
            object = session.get(MO, prm.mo_id)
            object.longitude = None
            session.add(object)
            line_mos = session.exec(
                select(MO).where(
                    or_(MO.point_a_id == object.id, MO.point_b_id == object.id)
                )
            ).all()
            for line_mo in line_mos:
                line_mo.geometry = None
                session.add(line_mo)
    if db_object_type.status == param_type.id:
        db_object_type.status = None
        session.add(db_object_type)
        session.info["disable_security"] = True
        params = session.exec(
            select(PRM).where(PRM.tprm_id == param_type.id)
        ).all()
        for prm in params:
            object = session.get(MO, prm.mo_id)
            object.status = None
            session.add(object)
