from copy import deepcopy
from datetime import datetime
from typing import Any

import math
from fastapi import HTTPException
from sqlalchemy import and_, text, cast, String, JSON, literal
from sqlmodel import Session, select

from common.common_constant import NAME_DELIMITER
from database import SQLALCHEMY_LIMIT
from functions.db_functions import db_delete
from functions.functions_utils.utils import (
    extract_location_data,
    set_location_attrs,
    find_deep_parent,
)
from models import TMO, MO, TPRM, PRM
from routers.object_router import utils
from routers.object_type_router.exceptions import (
    ObjectTypeNotExists,
    ObjectTypeNotValidAsConstraint,
    NotActualVersion,
    SeverityIdNotValid,
    PrimaryNotValid,
    LabelNotValid,
    ObjectTypeAlreadyExists,
    GeometryNotValid,
)
from routers.object_type_router.schemas import TMOCreate
from routers.parameter_type_router.exceptions import (
    ParameterTypeNotValidForPrimary,
    ParameterTypeNotValidForLabel,
    ParameterTypeNotValidForStatus,
)
from routers.parameter_type_router.utils import ParameterTypeDBGetter
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)
from val_types.constants import enum_val_type_name


def proceed_object_type_delete(session: Session, object_type: TMO, tmo_id: int):
    db_delete.delete_mo_links_by_tmo_id(session=session, object_type_id=tmo_id)
    db_delete.delete_prm_links_by_tmo_id(session=session, object_type_id=tmo_id)
    session.delete(object_type)
    session.commit()


def new_and_old_object_types_equal(
    new_object_type_data: dict, old_object_type_data: TMO
):
    copy_new_object_type_data = deepcopy(new_object_type_data)
    copy_old_object_type_data = deepcopy(old_object_type_data.dict())

    del copy_new_object_type_data["version"]
    del copy_old_object_type_data["version"]

    if copy_new_object_type_data == copy_old_object_type_data:
        return True

    return False


def validate_lifecycle_process_definition(
    lifecycle_process_definition: str,
) -> None:
    if isinstance(lifecycle_process_definition, str):
        parts = lifecycle_process_definition.split(":")

        if len(parts) != 2:
            raise HTTPException(
                status_code=422,
                detail="Lifecycle process definition need 2 parts: bpmn process id and bpmn version",
            )

        bpmn_process_id, version = parts
        if (
            bpmn_process_id == ""
            or bpmn_process_id.isspace()
            or bpmn_process_id.isdigit()
        ):
            raise HTTPException(
                status_code=422, detail="BPMN process ID have to  be string"
            )
        if not version.isdigit():
            raise HTTPException(
                status_code=422, detail="BPMN version  need to be integer"
            )
    else:
        raise HTTPException(
            status_code=422,
            detail="Invalid lifecycle_process_definition type. Parameter have to be string.",
        )


def set_names_for_objects_on_tmo_update(
    session: Session, old_object_type: TMO, new_primary: list[int]
):
    # In SQLAlchemy2 we can't load additional tables with the yield_per function. We should load PRMs in another query.
    query = (
        select(MO)
        .where(and_(MO.tmo_id == old_object_type.id, MO.active == True))  # noqa
        .execution_options(yield_per=SQLALCHEMY_LIMIT)
        # .options(selectinload(MO.prms))
    )
    objects = session.exec(query).partitions(SQLALCHEMY_LIMIT)
    names = []

    for obj_chunk in objects:  # type: list[MO]
        # we get p_ids, because they are using for MO name generation
        # get parent objects
        object_ids_with_p_ids = {
            obj.id: obj.p_id for obj in obj_chunk if obj.p_id is not None
        }
        if object_ids_with_p_ids:
            # yield per
            condition = [MO.id.in_(object_ids_with_p_ids.values())]
            stmt = (
                select(MO.id, MO.name)
                .where(*condition)
                .execution_options(yield_per=SQLALCHEMY_LIMIT)
            )
            parent_id_name = [data for data in session.exec(stmt)]

            if parent_id_name:
                parent_id_name = {
                    p_id: p_name for p_id, p_name in parent_id_name
                }
                for mo_id, parent_id in object_ids_with_p_ids.items():
                    if parent_id_name.get(parent_id, None) is not None:
                        object_ids_with_p_ids[mo_id] = parent_id_name.get(
                            parent_id
                        )  # noqa

        # if primary TPRM has val_type 'mo_link' - we need to add to name not id of linked MO, but real name,
        # so we need to get ids from PRM table and then get names by this ids
        primary_tprms_with_mo_links = session.exec(
            select(TPRM.id).where(
                TPRM.id.in_(new_primary), TPRM.val_type == "mo_link"
            )
        ).all()

        if primary_tprms_with_mo_links:
            mo_ids = [mo.id for mo in obj_chunk]
            mo_linked_names = {}
            # we get PRM`s, where we store ids of linked MO
            for chunk in range(math.ceil(len(mo_ids) / SQLALCHEMY_LIMIT)):
                offset = chunk * SQLALCHEMY_LIMIT
                limit = offset + SQLALCHEMY_LIMIT
                condition = [
                    PRM.mo_id.in_(mo_ids[offset:limit]),
                    PRM.tprm_id.in_(primary_tprms_with_mo_links),
                ]
                stmt = (
                    select(PRM.mo_id, PRM.value)
                    .where(*condition)
                    .execution_options(yield_per=SQLALCHEMY_LIMIT)
                )
                mo_linked_ids = [data for data in session.exec(stmt)]
                query = select(MO.id, MO.name).where(
                    MO.id.in_(
                        int(linked_prm[1]) for linked_prm in mo_linked_ids
                    )
                )
                linked_mos = session.exec(query)
                linked_mos = {
                    link[0]: link[1] for link in linked_mos.fetchall()
                }
                for linked_prm in mo_linked_ids:
                    mo_linked_names[linked_prm[1]] = linked_mos[
                        int(linked_prm[1])
                    ]

        for obj in obj_chunk:  # type: MO
            primary_values = []
            query = select(PRM).where(PRM.mo_id == obj.id)
            current_prms = session.exec(query).all()
            if not old_object_type.global_uniqueness and obj.p_id is not None:
                session.info["disable_security"] = True
                primary_values.append(object_ids_with_p_ids.get(obj.p_id))

            for primary_tprm in new_primary:
                for prm in current_prms:
                    if prm.tprm_id == primary_tprm:
                        if prm.tprm.val_type == "mo_link":
                            primary_values.append(
                                mo_linked_names.get(prm.value)
                            )
                        else:
                            primary_values.append(str(prm.value))
                        break

            name = NAME_DELIMITER.join(primary_values)
            if name in names:
                raise HTTPException(
                    status_code=422,
                    detail="Unable to set this primary value."
                    " Object names will be not unique.",
                )
            names.append(name)
            obj.name = name
            session.add(obj)


def set_labels_for_objects_on_tmo_update(
    session: Session, object_type_instance: TMO, new_label: list[int]
):
    # In SQLAlchemy2 we can't load additional tables with the yield_per function. We should load PRMs in another query.
    query = (
        select(MO)
        .where(and_(MO.tmo_id == object_type_instance.id, MO.active == True))  # noqa
        .execution_options(yield_per=SQLALCHEMY_LIMIT)
        # .options(selectinload(MO.prms))
    )
    objects = session.exec(query).partitions(SQLALCHEMY_LIMIT)

    for obj_chunk in objects:  # type: list[MO]
        # if label TPRM has val_type 'mo_link' - we need to add to name not id of linked MO, but real name,
        # so we need to get ids from PRM table and then get names by this ids
        label_tprms_with_mo_links = session.exec(
            select(TPRM.id).where(
                TPRM.id.in_(new_label), TPRM.val_type == "mo_link"
            )
        ).all()

        mo_linked_names = {}
        if label_tprms_with_mo_links:
            mo_ids = [mo.id for mo in obj_chunk]
            # we get PRM`s, where we store ids of linked MO
            for chunk in range(math.ceil(len(mo_ids) / SQLALCHEMY_LIMIT)):
                offset = chunk * SQLALCHEMY_LIMIT
                limit = offset + SQLALCHEMY_LIMIT
                condition = [
                    PRM.mo_id.in_(mo_ids[offset:limit]),
                    PRM.tprm_id.in_(label_tprms_with_mo_links),
                ]
                stmt = select(PRM.mo_id, PRM.value).where(*condition)
                mo_linked_ids = [data for data in session.exec(stmt)]
                query = select(MO.id, MO.name).where(
                    MO.id.in_(
                        int(linked_prm[1]) for linked_prm in mo_linked_ids
                    )
                )
                linked_mos = session.execute(query)
                linked_mos = {
                    link[0]: link[1] for link in linked_mos.fetchall()
                }
                for linked_prm in mo_linked_ids:
                    mo_linked_names[linked_prm[1]] = linked_mos[
                        int(linked_prm[1])
                    ]

        for obj in obj_chunk:  # type: MO
            label_values = []
            query = select(PRM).where(PRM.mo_id == obj.id)
            current_prms = session.exec(query).all()
            prms = {i.tprm_id: i for i in current_prms}
            for label_tprm in new_label:
                label_prm = prms.get(label_tprm, None)
                if not label_prm:
                    # This is a critical moment. Since the label should only include required fields.
                    continue
                if label_prm.tprm.val_type == "mo_link":
                    label_values.append(
                        mo_linked_names.get(label_prm.value, "")
                    )
                else:
                    label_values.append(str(label_prm.value))

            label = NAME_DELIMITER.join(label_values)
            obj.label = label if label else None
            session.add(obj)

    session.commit()


class ObjectTypeDBGetter(ParameterTypeDBGetter):
    def __init__(self, session: Session):
        super().__init__(session=session)
        self._session = session

    def _get_object_type_instance_by_id(
        self, object_type_id: int
    ) -> TMO | None:
        query = select(TMO).where(TMO.id == object_type_id)
        object_instance = self._session.execute(query).scalar()

        if object_instance:
            return object_instance

        raise ObjectTypeNotExists(
            status_code=404,
            detail=f"Object type with id {object_type_id} not found.",
        )

    def _get_object_type_instance_by_name(
        self, object_type_name: str, raise_error: bool = True
    ) -> TMO | None:
        query = select(TMO).where(TMO.name == object_type_name)
        object_instance = self._session.execute(query).scalar()

        if object_instance or not raise_error:
            return object_instance

        raise ObjectTypeNotExists(
            status_code=422,
            detail=f"Object type with name {object_type_name} not found.",
        )

    @staticmethod
    def _object_type_parent_validation(session: Session, parent_id: int):
        if parent_id:
            db_parent_object_type = session.get(TMO, parent_id)
            if not db_parent_object_type:
                raise ObjectTypeNotExists(
                    status_code=422,
                    detail=f"Invalid parent id. Object with id {parent_id} not found.",
                )

    @staticmethod
    def _object_type_points_constraint_validation(
        session: Session, points_constraint_by_tmo: list[int]
    ):
        # 'points_constraint_by_tmo' stores list of tmo ids, and feature point_a\b have to be in these TMO`s
        if points_constraint_by_tmo:
            query = select(TMO.id).where(TMO.id.in_(points_constraint_by_tmo))
            object_type_instances = session.execute(query).scalars().all()

            constraint_object_types = set(points_constraint_by_tmo)
            if object_type_instances is None:
                object_type_instances = []

            # if there TMO`s which are not exists -- we need to raise error
            not_valid_object_type_ids = constraint_object_types.difference(
                set(object_type_instances)
            )
            if not_valid_object_type_ids:
                raise ObjectTypeNotValidAsConstraint(
                    status_code=422,
                    detail="There are TMO`s, which can't be used by constraint, because of they are not "
                    f"exists: {not_valid_object_type_ids}",
                )

    def _create_object_type(self, object_type_to_create: TMOCreate) -> TMO:
        self._object_type_parent_validation(
            session=self._session, parent_id=object_type_to_create.p_id
        )

        self._object_type_points_constraint_validation(
            session=self._session,
            points_constraint_by_tmo=object_type_to_create.points_constraint_by_tmo,
        )

        object_type = object_type_to_create.dict()
        object_type["created_by"] = get_username_from_session(
            session=self._session
        )
        object_type["modified_by"] = get_username_from_session(
            session=self._session
        )
        return TMO(**object_type)

    def __object_severity_id_validation(
        self, severity_id: Any, object_type_instance: TMO
    ):
        if severity_id:
            parameter_type_instance = self._get_parameter_type_instance_by_id(
                parameter_type_id=severity_id
            )

            if parameter_type_instance.tmo_id != object_type_instance.id:
                raise SeverityIdNotValid(
                    status_code=409,
                    detail="Severity id it's TPRM id, which need to be from current TMO.",
                )

    def __process_primary_remove(
        self, primary: list[int], object_type_instance: TMO
    ):
        if len(primary) == 0:
            query = select(MO).where(MO.tmo_id == object_type_instance.id)
            object_instances = self._session.exec(query).all()

            for object_instance in object_instances:
                object_instance.name = str(object_instance.id)
                self._session.add(object_instance)

    def __process_primary_set(
        self,
        object_type_instance: TMO,
        primary: list[int],
    ):
        for parameter_type_id in primary:
            query = select(TPRM).where(
                TPRM.id == parameter_type_id,
                TPRM.required == True,  # noqa
                TPRM.multiple != True,  # noqa
                TPRM.val_type.in_(
                    ["str", "int", "float", "mo_link", "formula"]
                ),
                TPRM.tmo_id == object_type_instance.id,
            )
            is_valid = self._session.exec(query).first()
            if is_valid:
                continue

            raise ParameterTypeNotValidForPrimary(
                status_code=422,
                detail=f"Invalid param type id in primary field: {parameter_type_id}.",
            )

        set_names_for_objects_on_tmo_update(
            session=self._session,
            old_object_type=object_type_instance,
            new_primary=primary,
        )

    def __object_type_primary_validation(
        self,
        primary: Any,
        object_type_instance: TMO,
    ):
        if primary:
            if not isinstance(primary, list):
                raise PrimaryNotValid(
                    status_code=422, detail="Primary field should be list."
                )

            self.__process_primary_remove(
                primary=primary, object_type_instance=object_type_instance
            )

            self.__process_primary_set(
                primary=primary,
                object_type_instance=object_type_instance,
            )

    def __object_type_label_validation(
        self,
        label: Any,
        object_type_instance: TMO,
    ):
        if label:
            if not isinstance(label, list):
                raise LabelNotValid(
                    status_code=422, detail="Label field should be list."
                )

            for parameter_type_id in label:
                query = select(TPRM).where(
                    TPRM.id == parameter_type_id,
                    TPRM.required == True,  # noqa
                    TPRM.multiple != True,  # noqa
                    TPRM.val_type.in_(
                        ["str", "int", "float", "mo_link", "formula"]
                    ),
                    TPRM.tmo_id == object_type_instance.id,
                )
                is_valid = self._session.exec(query).first()
                if not is_valid:
                    raise ParameterTypeNotValidForLabel(
                        status_code=422,
                        detail=f"Invalid param type id in label field: {parameter_type_id}.",
                    )

            set_labels_for_objects_on_tmo_update(
                session=self._session,
                object_type_instance=object_type_instance,
                new_label=label,
            )
            return

    def __object_type_name_validation(
        self, name: Any, object_type_instance: TMO
    ):
        if name:
            query = select(TMO).where(
                TMO.name == name,
                TMO.id != object_type_instance.id,
            )
            duplicated_object_type_instance = self._session.exec(query).first()
            if duplicated_object_type_instance:
                raise ObjectTypeAlreadyExists(
                    status_code=422,
                    detail=f"Object type parameter already exists with name: {name}",
                )

    def __rebuild_location_for_child_objects(self, object_type_instance: TMO):
        query = select(TMO).where(TMO.p_id == object_type_instance.id)
        children_object_type_instances = self._session.exec(query).all()

        query = select(MO).where(MO.tmo_id == object_type_instance.id)
        all_parent_object_instances = self._session.exec(query).all()

        for child_object_type_instance in children_object_type_instances:
            if child_object_type_instance.inherit_location:
                for parent_object_instance in all_parent_object_instances:
                    query = select(MO).where(
                        MO.tmo_id == child_object_type_instance.id,
                        MO.p_id == parent_object_instance.id,
                    )
                    child_object_instances = self._session.exec(query).all()

                    location_data = extract_location_data(
                        geometry_type=object_type_instance.geometry_type,
                        parent_mo=parent_object_instance,
                    )
                    set_location_attrs(
                        session=self._session,
                        db_param=object_type_instance.geometry_type,
                        child_mos=child_object_instances,
                        set_value=True,
                        location_data=location_data,
                    )

    def __object_type_location_validation(
        self,
        latitude: Any,
        longitude: Any,
        inherit_location: Any,
        geometry_type: Any,
        object_type_instance: TMO,
    ):
        if latitude:
            query = select(TPRM).where(
                TPRM.id == latitude,
                TPRM.tmo_id == object_type_instance.id,
            )
            latitude_parameter_type_instance = self._session.exec(query).first()

            if not latitude_parameter_type_instance:
                raise GeometryNotValid(
                    status_code=422,
                    detail="Invalid latitude value. Please, pass correct param type id.",
                )
            if latitude_parameter_type_instance.val_type != "float":
                raise GeometryNotValid(
                    status_code=422,
                    detail="Latitude param type should be float.",
                )
            if latitude_parameter_type_instance.multiple:
                raise GeometryNotValid(
                    status_code=422,
                    detail="Latitude param type should be not multiple.",
                )

            query = select(PRM).where(
                PRM.tprm_id == latitude_parameter_type_instance.id
            )
            latitude_parameters = self._session.exec(query).all()

            updated_object_ids = []
            for latitude_parameter in latitude_parameters:
                object_instance = self._session.get(
                    MO, latitude_parameter.mo_id
                )
                object_instance.latitude = float(latitude_parameter.value)
                updated_object_ids.append(latitude_parameter.mo_id)
                self._session.add(object_instance)

            if object_type_instance.latitude:
                query = select(PRM).where(
                    PRM.tprm_id == object_type_instance.latitude,
                    PRM.mo_id.notin_(updated_object_ids),
                )
                old_latitude_parameters = self._session.exec(query).all()

                for latitude_parameter in old_latitude_parameters:
                    object_instance = self._session.get(
                        MO, latitude_parameter.mo_id
                    )
                    object_instance.latitude = None
                    self._session.add(object_instance)

            self.__rebuild_location_for_child_objects(
                object_type_instance=object_type_instance
            )

        if longitude:
            query = select(TPRM).where(
                TPRM.id == longitude,
                TPRM.tmo_id == object_type_instance.id,
            )
            longitude_parameter_type_instance = self._session.exec(
                query
            ).first()

            if not longitude_parameter_type_instance:
                raise GeometryNotValid(
                    status_code=422,
                    detail="Invalid longitude value. Please, pass correct param type id.",
                )
            if longitude_parameter_type_instance.val_type != "float":
                raise GeometryNotValid(
                    status_code=422,
                    detail="Longitude param type should be float.",
                )
            if longitude_parameter_type_instance.multiple:
                raise GeometryNotValid(
                    status_code=422,
                    detail="Longitude param type should be not multiple.",
                )

            query = select(PRM).where(
                PRM.tprm_id == longitude_parameter_type_instance.id
            )
            longitude_parameters = self._session.exec(query).all()

            updated_object_ids = []
            for longitude_parameter in longitude_parameters:
                object_instance = self._session.get(
                    MO, longitude_parameter.mo_id
                )
                object_instance.longitude = float(longitude_parameter.value)
                updated_object_ids.append(longitude_parameter.mo_id)
                self._session.add(object_instance)

            if object_type_instance.longitude:
                query = select(PRM).where(
                    PRM.tprm_id == object_type_instance.longitude,
                    PRM.mo_id.notin_(updated_object_ids),
                )
                old_longitude_parameters = self._session.exec(query).all()
                for longitude_parameter in old_longitude_parameters:
                    object_instance = self._session.get(
                        MO, longitude_parameter.mo_id
                    )
                    object_instance.longitude = None
                    self._session.add(object_instance)

            self.__rebuild_location_for_child_objects(
                object_type_instance=object_type_instance
            )

        if inherit_location is not None:
            if not object_type_instance.geometry_type:
                raise GeometryNotValid(
                    status_code=422,
                    detail="Unable to set inherit location for object type without geometry type.",
                )
            if not object_type_instance.p_id:
                raise GeometryNotValid(
                    status_code=422,
                    detail="Unable to set inherit location for object type without parent.",
                )

            query = select(MO).where(MO.tmo_id == object_type_instance.id)
            object_instances = self._session.execute(query).scalars()

            for object_instance in object_instances:
                if not object_instance.p_id:
                    continue

                if inherit_location:
                    deep_parent_mo: MO = find_deep_parent(
                        session=self._session,
                        object_instance=object_instance,
                        object_type_instance=object_type_instance,
                        from_parent=True,
                    )[1]

                    if not deep_parent_mo:
                        continue

                    location_data = extract_location_data(
                        geometry_type=object_type_instance.geometry_type,
                        parent_mo=deep_parent_mo,
                    )
                    set_location_attrs(
                        session=self._session,
                        db_param=object_type_instance.geometry_type,
                        child_mos=[object_instance],
                        set_value=True,
                        location_data=location_data,
                    )
                    continue

                set_location_attrs(
                    session=self._session,
                    db_param=object_type_instance.geometry_type,
                    child_mos=[object_instance],
                )

        if geometry_type:
            # Update information for mo if line updated
            if (
                object_type_instance.geometry_type
                and object_type_instance.geometry_type == "line"
            ):
                query = text(
                    f"""
                SELECT mo.*
                  FROM mo, json_each(mo.geometry)
                 WHERE mo.geometry::varchar != 'null' AND
                       mo.point_a_id IS NOT NULL AND
                       mo.point_b_id IS NOT NULL AND
                       tmo_id = {object_type_instance.id} AND
                       key = 'path' AND
                       json_typeof(value) != 'string'"""
                )
                query = select(MO).from_statement(query)
                incorrect_object_instances = (
                    self._session.exec(query).scalars().all()
                )

                query = select(MO).where(
                    and_(
                        MO.tmo_id == object_type_instance.id,
                        MO.point_a_id.is_not(None),
                        MO.point_b_id.is_not(None),
                        cast(MO.geometry, String)
                        == cast(literal(JSON.NULL, JSON()), String),
                    )
                )
                incorrect_object_instances.extend(
                    self._session.exec(query).all()
                )

                for object_instance in incorrect_object_instances:
                    object_instance.geometry = utils.update_geometry(
                        object_instance=object_instance,
                        point_a=object_instance.point_a,
                        point_b=object_instance.point_b,
                    )
                    object_instance.version += 1
                    self._session.add(object_instance)

                self._session.commit()

    def __object_type_status_validation(
        self, status: Any, object_type_instance: TMO
    ):
        if status:
            query = select(TPRM).where(
                TPRM.id == status,
                TPRM.tmo_id == object_type_instance.id,
            )
            status_param_type = self._session.exec(query).first()
            if not status_param_type:
                raise ParameterTypeNotValidForStatus(
                    status_code=422,
                    detail="Invalid status value. Please, pass correct param type id.",
                )

            if status_param_type.val_type not in [
                "str",
                "int",
                "float",
                enum_val_type_name,
            ]:
                raise ParameterTypeNotValidForStatus(
                    status_code=422,
                    detail="Status param type should be str, int or float.",
                )
            if status_param_type.multiple:
                raise ParameterTypeNotValidForStatus(
                    status_code=422,
                    detail="Status param type should be not multiple.",
                )

            query = select(PRM).where(PRM.tprm_id == status_param_type.id)
            status_parameters = self._session.exec(query).all()

            updated_mo_ids = []
            for parameter_instance in status_parameters:
                object_instance = self._session.get(
                    MO, parameter_instance.mo_id
                )
                object_instance.status = parameter_instance.value
                updated_mo_ids.append(parameter_instance.mo_id)
                self._session.add(object_instance)

            if object_type_instance.status:
                query = select(PRM).where(
                    PRM.tprm_id == object_type_instance.status,
                    PRM.mo_id.notin_(updated_mo_ids),
                )
                prev_status_prms = self._session.exec(query).all()

                for parameter_instance in prev_status_prms:
                    object_instance = self._session.get(
                        MO, parameter_instance.mo_id
                    )
                    object_instance.status = None
                    self._session.add(object_instance)

    def _update_object_type(
        self, object_type_instance: TMO, object_type_to_update: dict
    ):
        if object_type_instance.version != object_type_to_update["version"]:
            raise NotActualVersion(
                status_code=409,
                detail=f"Actual version of TMO: {object_type_instance.version}.",
            )

        self.__object_severity_id_validation(
            severity_id=object_type_to_update.get("severity_id"),
            object_type_instance=object_type_instance,
        )

        self._object_type_points_constraint_validation(
            session=self._session,
            points_constraint_by_tmo=object_type_to_update.get(
                "points_constraint_by_tmo"
            ),
        )

        self.__object_type_primary_validation(
            primary=object_type_to_update.get("primary"),
            object_type_instance=object_type_instance,
        )

        self.__object_type_label_validation(
            label=object_type_to_update.get("label"),
            object_type_instance=object_type_instance,
        )

        self.__object_type_name_validation(
            name=object_type_to_update.get("name"),
            object_type_instance=object_type_instance,
        )

        self._object_type_parent_validation(
            session=self._session, parent_id=object_type_to_update.get("p_id")
        )

        self.__object_type_location_validation(
            latitude=object_type_to_update.get("latitude"),
            longitude=object_type_to_update.get("longitude"),
            inherit_location=object_type_to_update.get("inherit_location"),
            geometry_type=object_type_to_update.get("geometry_type"),
            object_type_instance=object_type_instance,
        )

        self.__object_type_status_validation(
            status=object_type_to_update.get("status"),
            object_type_instance=object_type_instance,
        )

        object_type_to_update["version"] += 1

        for key, value in object_type_to_update.items():
            setattr(object_type_instance, key, value)

        object_type_instance.modification_date = datetime.utcnow()
        return object_type_instance

    def get_breadcrumbs_for_object_type(self, object_type_instance: TMO):
        """Return ordered full chain list of parents"""
        if object_type_instance.p_id is None:
            return [object_type_instance]
        stmt = select(TMO).where(TMO.id == object_type_instance.p_id)
        child_object_type_instance = self._session.exec(stmt).first()

        return self.get_breadcrumbs_for_object_type(
            object_type_instance=child_object_type_instance
        ) + [object_type_instance]
