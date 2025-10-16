import copy
import io
import itertools
import pickle
from collections import defaultdict
from datetime import datetime, timezone
from pprint import pprint
from urllib.parse import urlparse

import grpc
from fastapi.responses import StreamingResponse
from google.protobuf import json_format
from sqlalchemy import cast, Integer, func, or_, and_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select, Session as sqlmodel_Session

from common.common_constant import NAME_DELIMITER
from config.grpc_config import ZEEBE_GRPC_PORT, ZEEBE_GRPC_HOST
from config.minio_config import MINIO_BUCKET
from database import get_chunked_values_by_sqlalchemy_limit
from functions.db_functions.db_create import create_db_object
from functions.db_functions.db_read import (
    get_object_with_parameters,
    get_parameters_for_object_by_object_query,
    get_objects_with_parameters,
)
from functions.functions_utils.utils import (
    set_location_attrs,
    extract_location_data,
    calculate_by_formula_new,
    find_deep_parent,
    count_objects,
)
from functions.validation_functions.validation_function import (
    check_if_all_required_params_passed,
    object_data_validation_when_update,
)
from models import MO, PRM, TPRM, TMO, GeometryType
from routers.object_router.exceptions import (
    NotActualVersion,
    UpdatedObjectDataHasNoDifferenceWithOriginal,
    DuplicatedObjectName,
    ObjectCanNotBeParent,
    ObjectNotExists,
    ObjectCanNotBeArchived,
    CanNotDeletePrimaryObject,
    PointInstanceNotExists,
    ObjectCustomException,
    DuplicatedParameter,
)
from routers.object_router.schemas import (
    GetObjectRouteRequest,
    AddModelToObjectRequest,
    UpdateObjectRequest,
    DeleteObjectRequest,
    MOCreateWithParams,
    NewObjectName,
    GetChildObjectsWithProcessInstanceIdRequest,
    MassiveObjectsUpdate,
    GetSiteFiberRequest,
    GetObjectsByNamesRequest,
    GetObjectsByNamesResponse,
    GetObjectWithGroupedParametersRequest,
    GetLinkedObjectsByParametersLinkRequest,
    ParameterDataWithObject,
    LinkedParameterInstances,
    MassiveObjectDeleteRequest,
    GetAllParentsForObjectRequest,
    GetParentInheritLocationRequest,
    MOInheritParent,
    GetObjectsByParameterRequest,
    GetObjectsByParameterResponse,
    GetObjectsByObjectTypeRequest,
    GetObjectsByObjectTypeResponse,
    GetObjectWithParametersRequest,
    RebuildGeometryRequest,
    GetAllParentsForObjectMassiveRequest,
)
from routers.object_router.utils import (
    ObjectDBGetter,
    get_value_for_sequence,
    update_child_location_mo,
    update_geometry,
    collapse_sequences_if_exist,
    proceed_object_delete,
    validate_object_parameters,
    proceed_parameter_attributes,
    get_grouped_params,
    decode_pickle_data,
    check_mo_is_part_of_other_mo_name,
    proceed_object_list_delete,
    recursive_find_children_all_children_tmo,
    get_conditions_for_coords,
    reconstruct_geometry,
)
from routers.object_type_router.exceptions import ObjectTypeHasNoParent
from routers.object_type_router.utils import ObjectTypeDBGetter
from routers.parameter_router.exceptions import (
    NotValidFormulaParameterValue,
    NotValidParameterValue,
)
from routers.parameter_router.schemas import (
    PRMCreateByMO,
    ResponseGroupedParams,
    GroupedParam,
)
from routers.parameter_router.utils import ParameterDBGetter
from routers.parameter_type_router.utils import ParameterTypeDBGetter
from services.grpc_service.proto_files.severity.files import (
    zeebe_severity_pb2,
    zeebe_severity_pb2_grpc,
)
from services.minio_service.minio_client import minio_client
from services.security_service.routers.utils.recursion import (
    get_items_recursive_up,
)


class GetObjectRoute(ObjectDBGetter):
    def __init__(self, session: Session, request: GetObjectRouteRequest):
        self._session = session
        super().__init__(session=session)

        self._request = request
        self._object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

    def execute(self):
        route_list = self._get_object_route(
            db_object=self._object_instance, route_list=[]
        )
        return route_list


class AddModelToObject(ObjectDBGetter):
    def __init__(self, session: Session, request: AddModelToObjectRequest):
        self._session = session
        self._request = request
        super().__init__(session=session)

        self._object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

    async def _add_new_model_to_object(self):
        file_bytes = await self._request.file.read()
        buf = io.BytesIO(file_bytes)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=self._request.file.filename,
            data=buf,
            length=buf.getbuffer().nbytes,
        )

        file_link_in_minio = minio_client.presigned_get_object(
            MINIO_BUCKET, self._request.file.filename
        )
        self._object_instance.model = file_link_in_minio
        self._object_instance.version = self._object_instance.version + 1
        self._object_instance.modification_date = datetime.now(timezone.utc)
        self._session.add(self._object_instance)
        self._session.commit()

    async def _delete_model_from_object(self):
        file_url = self._object_instance.model
        parsed_url = urlparse(file_url)
        filename = parsed_url.path.split("/")[-1]

        minio_client.remove_object(MINIO_BUCKET, filename)
        self._object_instance.model = None
        self._object_instance.version = self._object_instance.version + 1
        self._object_instance.modification_date = datetime.now(timezone.utc)
        self._session.add(self._object_instance)
        self._session.commit()

    async def execute(self):
        if self._request.file:
            await self._add_new_model_to_object()
            return {"status": "ok", "detail": "File was saved. Model updated"}

        else:
            if self._object_instance.model:
                await self._delete_model_from_object()
                self._object_instance.version = (
                    self._object_instance.version + 1
                )
                self._object_instance.modification_date = datetime.now(
                    timezone.utc
                )
                return {
                    "status": "ok",
                    "detail": "File was deleted. Model updated tp None",
                }

            return self._object_instance


class UpdateObject(ObjectDBGetter):
    def __init__(
        self,
        session: Session,
        request: UpdateObjectRequest,
        autocommit: bool = True,
    ):
        self._session = session
        self._request = request
        self._autocommit = autocommit
        super().__init__(session=session)

        self._object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

    @staticmethod
    def _check_difference_of_update(
        exists_object: MO, object_to_update: UpdateObjectRequest
    ):
        exists_object_instance = exists_object.dict()
        object_to_update = object_to_update.dict(exclude_unset=True)

        exclude = ["version", "object_id"]

        is_object_updated = all(
            exists_object_instance.get(attribute) == value
            for attribute, value in object_to_update.items()
            if attribute not in exclude
        )

        if is_object_updated:
            raise UpdatedObjectDataHasNoDifferenceWithOriginal(
                status_code=422,
                detail="Object for update has no difference, compare to original",
            )

        return True

    def _version_difference_check(self):
        if self._object_instance.version != self._request.version:
            raise NotActualVersion(
                status_code=409,
                detail=f"Actual version of MO: {self._object_instance.version}.",
            )

    def check(self):
        self._version_difference_check()
        self._check_difference_of_update(
            exists_object=self._object_instance, object_to_update=self._request
        )

    @staticmethod
    def _update_active_attribute(object_to_update: dict):
        match object_to_update.get("active"):
            case False:
                raise ObjectCanNotBeArchived(
                    status_code=422,
                    detail="Unable to archive object in this way. Use DELETE method.",
                )

            case None:
                if "active" in object_to_update:
                    del object_to_update["active"]

    def _check_parent_can_be_set(
        self, object_to_update: dict, parent_object_instance: MO
    ):
        if not parent_object_instance:
            raise ObjectNotExists(
                status_code=422,
                detail=f"Invalid parent id ({object_to_update['p_id']}). Object does not exist.",
            )

        if self._object_type_instance.p_id is None:
            raise ObjectTypeHasNoParent(
                status_code=422,
                detail=f"Its impossible to create parent link, because TMO "
                f"with id {self._object_type_instance.id} has no parent",
            )

        if self._object_type_instance.p_id != parent_object_instance.tmo_id:
            raise ObjectCanNotBeParent(
                status_code=422,
                detail=f"Invalid parent id ({object_to_update['p_id']})."
                f" Parent should be object of object type"
                f" with id {self._object_type_instance.p_id}.",
            )

    def _update_object_names_by_changed_parent(
        self, parent_object_instance: MO, object_instance: MO
    ):
        if self._object_type_instance.primary:
            object_name_parts = []
            if not self._object_type_instance.global_uniqueness:
                object_name_parts.append(parent_object_instance.name)

            for parameter_type_id in self._object_type_instance.primary:
                for parameter in object_instance.prms:
                    if parameter.tprm_id == parameter_type_id:
                        query = select(TPRM.val_type).where(
                            TPRM.id == parameter_type_id
                        )
                        parameter_type_val_type = self._session.exec(
                            query
                        ).first()

                        prm_value = str(parameter.value)
                        if parameter_type_val_type == "mo_link":
                            query = select(MO.name).where(
                                MO.id == int(parameter.value)
                            )
                            prm_value = self._session.exec(query).first()

                        object_name_parts.append(prm_value)

            updated_object_name = NAME_DELIMITER.join(object_name_parts)
            self._session.info["disable_security"] = True

            query = select(MO).where(
                MO.id != object_instance.id,
                MO.name == updated_object_name,
                MO.tmo_id == self._object_type_instance.id,
            )

            this_name_already_exist = self._session.exec(query).first()

            if this_name_already_exist:
                raise DuplicatedObjectName(
                    status_code=422,
                    detail="Unable to set this parent. Object names will be not unique.",
                )

            return updated_object_name

        return str(object_instance.id)

    @staticmethod
    def _update_line_data_by_point_a_and_point_b(
        session: Session,
        object_data_to_update: dict,
        object_type_instance: TMO,
        object_instance: MO,
    ):
        # GET POINT A
        conditions = [MO.id == object_data_to_update["point_a_id"]]

        if object_type_instance.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type_instance.points_constraint_by_tmo)
            )

        query = select(MO).where(*conditions)
        new_point_a: MO | None = session.execute(query).scalar_one_or_none()

        # GET POINT B
        conditions = [MO.id == object_data_to_update["point_b_id"]]
        if object_type_instance.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type_instance.points_constraint_by_tmo)
            )
        query = select(MO).where(*conditions)
        new_point_b: MO | None = session.execute(query).scalar_one_or_none()

        object_instance.geometry = update_geometry(
            object_instance=object_instance,
            point_a=new_point_a,
            point_b=new_point_b,
        )

        flag_modified(instance=object_instance, key="geometry")

        data = {
            "point_a_id": new_point_a.id if new_point_a else None,
            "point_b_id": new_point_b.id if new_point_b else None,
            "geometry": object_instance.geometry,
        }

        update_child_location_mo(
            session=session,
            object_instance=object_instance,
            object_type_instance=object_type_instance,
            new_data=data,
        )
        object_data_to_update["geometry"] = object_instance.geometry

    @staticmethod
    def _update_point_a(
        session: Session,
        object_data_to_update: dict,
        object_type_instance: TMO,
        object_instance: MO,
    ):
        conditions = [MO.id == object_data_to_update["point_a_id"]]
        if object_type_instance.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type_instance.points_constraint_by_tmo)
            )

        query = select(MO).where(*conditions)
        new_point_a: MO | None = session.execute(query).scalar_one_or_none()

        if new_point_a and object_instance.point_b_id:
            object_instance.geometry = update_geometry(
                object_instance=object_instance,
                point_a=new_point_a,
                point_b=object_instance.point_b,
            )
            flag_modified(instance=object_instance, key="geometry")

        data = {
            "point_a_id": new_point_a.id if new_point_a else None,
            "geometry": object_instance.geometry,
        }

        update_child_location_mo(
            session=session,
            object_instance=object_instance,
            object_type_instance=object_type_instance,
            new_data=data,
        )

    @staticmethod
    def _update_point_b(
        session: Session,
        object_data_to_update: dict,
        object_type_instance: TMO,
        object_instance: MO,
    ):
        conditions = [MO.id == object_data_to_update["point_b_id"]]
        if object_type_instance.points_constraint_by_tmo:
            conditions.append(
                MO.tmo_id.in_(object_type_instance.points_constraint_by_tmo)
            )

        query = select(MO).where(*conditions)
        new_point_b: MO | None = session.execute(query).scalar_one_or_none()

        if new_point_b and object_instance.point_a_id:
            object_instance.geometry = update_geometry(
                object_instance=object_instance,
                point_a=object_instance.point_a,
                point_b=new_point_b,
            )
            flag_modified(instance=object_instance, key="geometry")

        data = {
            "point_b_id": new_point_b.id if new_point_b else None,
            "geometry": object_instance.geometry,
        }
        update_child_location_mo(
            session=session,
            object_instance=object_instance,
            object_type_instance=object_type_instance,
            new_data=data,
        )

    def _update_point_data(
        self,
        object_data_to_update: dict,
        object_type_instance: TMO,
        object_instance: MO,
    ):
        path_type = (
            object_instance.geometry
            and object_instance.geometry.get("path")
            and object_instance.geometry["path"].get("type")
        )
        if path_type and path_type != "LineString":
            raise ObjectCustomException(
                status_code=422,
                detail="Mismatched TMO geometry type and MO geometry path type.",
            )

        both_point_a_and_point_b_updated = (
            "point_a_id" in object_data_to_update
            and "point_b_id" in object_data_to_update
            and object_type_instance.geometry_type == GeometryType.line
        )

        if both_point_a_and_point_b_updated:
            self._update_line_data_by_point_a_and_point_b(
                session=self._session,
                object_instance=object_instance,
                object_type_instance=object_type_instance,
                object_data_to_update=object_data_to_update,
            )

        elif (
            "point_a_id" in object_data_to_update
            and object_type_instance.geometry_type == GeometryType.line
        ):
            self._update_point_a(
                session=self._session,
                object_data_to_update=object_data_to_update,
                object_type_instance=object_type_instance,
                object_instance=object_instance,
            )

        elif (
            "point_b_id" in object_data_to_update
            and object_type_instance.geometry_type == GeometryType.line
        ):
            self._update_point_b(
                session=self._session,
                object_data_to_update=object_data_to_update,
                object_type_instance=object_type_instance,
                object_instance=object_instance,
            )

    def _update_geometry(
        self,
        object_data_to_update: dict,
        object_instance: MO,
        object_type_instance: TMO,
    ):
        if "geometry" in object_data_to_update:
            update_child_location_mo(
                session=self._session,
                object_instance=object_instance,
                object_type_instance=object_type_instance,
                new_data={"geometry": object_data_to_update["geometry"]},
            )

    @staticmethod
    def _update_label(
        object_instance: MO,
        object_type_instance: TMO,
    ):
        if object_type_instance.label:
            label_values = []
            for tprm_id in object_type_instance.label:
                for prm in object_instance.prms:
                    if prm.tprm_id == tprm_id:
                        label_values.append(str(prm.value))
            name = NAME_DELIMITER.join(label_values)
            return name
        else:
            return None

    @staticmethod
    def _update_sequence(
        session: Session,
        object_data_to_update: dict,
        object_instance: MO,
    ):
        # initialize new value for sequence if object unarchived
        if object_data_to_update.get("active") and not object_instance.active:
            query = select(TPRM).where(
                TPRM.tmo_id == object_instance.tmo_id,
                TPRM.val_type == "sequence",
            )
            sequence_tprms = session.execute(query)
            sequence_tprms = sequence_tprms.scalars().all()
            for sequence_tprm in sequence_tprms:
                seq_type = None
                if sequence_tprm.constraint:
                    query = select(PRM.value).where(
                        PRM.mo_id == object_instance.id,
                        PRM.tprm_id == int(sequence_tprm.constraint),
                    )
                    seq_type = session.execute(query)
                    seq_type = seq_type.scalar()

                seq_value = get_value_for_sequence(
                    session, sequence_tprm, seq_type
                )

                # check if sequence already exists
                stmt = select(PRM).where(
                    PRM.tprm_id == sequence_tprm.id,
                    PRM.mo_id == object_instance.id,
                )
                sequence_exists = session.execute(stmt).scalar()
                if sequence_exists:
                    sequence_exists.value = seq_value
                    sequence_exists.version = 1
                else:
                    sequence_exists = PRM(
                        tprm_id=sequence_tprm.id,
                        value=seq_value,
                        mo_id=object_instance.id,
                    )

                session.add(sequence_exists)

    def _update_inherit_location(
        self,
        object_data_to_update: dict,
        object_instance: MO,
    ):
        if (
            "p_id" in object_data_to_update
            and object_data_to_update["p_id"] is not None
        ):
            query = select(MO).where(MO.id == object_data_to_update["p_id"])
            parent_object_instance = self._session.execute(query).scalar()

            if object_instance.tmo.inherit_location:
                location_data = extract_location_data(
                    geometry_type=self._object_type_instance.geometry_type,
                    parent_mo=parent_object_instance,
                )
                set_location_attrs(
                    session=self._session,
                    db_param=self._object_type_instance.geometry_type,
                    child_mos=[object_instance],
                    set_value=True,
                    location_data=location_data,
                )

                return

            if object_instance.tmo.inherit_location:
                set_location_attrs(
                    session=self._session,
                    db_param=self._object_type_instance.geometry_type,
                    child_mos=[object_instance],
                    set_value=True,
                    location_data={},
                )

    def _update_object_attributes(
        self,
        object_to_update: dict,
        object_instance: MO,
    ):
        object_to_update["version"] += 1

        self._update_active_attribute(object_to_update=object_to_update)

        object_to_update["name"] = self._update_parent_id_attribute(
            object_to_update=object_to_update, object_instance=object_instance
        )

        self._update_inherit_location(
            object_data_to_update=object_to_update,
            object_instance=object_instance,
        )

        self._update_point_data(
            object_instance=object_instance,
            object_type_instance=self._object_type_instance,
            object_data_to_update=object_to_update,
        )

        self._update_geometry(
            object_instance=object_instance,
            object_type_instance=self._object_type_instance,
            object_data_to_update=object_to_update,
        )

        object_to_update["label"] = self._update_label(
            object_instance=object_instance,
            object_type_instance=self._object_type_instance,
        )

    @staticmethod
    def _get_updated_object_instance(
        object_to_update: dict, object_instance: MO
    ) -> MO:
        for key, value in object_to_update.items():
            setattr(object_instance, key, value)

        return object_instance

    def _update_object_data(
        self, object_instance: MO, object_to_update: UpdateObjectRequest
    ):
        object_to_update = object_to_update.dict(exclude_unset=True)
        self._session.info["disable_security"] = True

        self._object_type_instance = (
            self._object_type_db_getter._get_object_type_instance_by_id(
                object_type_id=object_instance.tmo_id
            )
        )

        self._update_object_attributes(
            object_instance=object_instance, object_to_update=object_to_update
        )

        self._update_sequence(
            session=self._session,
            object_data_to_update=object_to_update,
            object_instance=object_instance,
        )

        del object_to_update["object_id"]

        self._get_updated_object_instance(
            object_to_update=object_to_update, object_instance=object_instance
        )

    def _update_object_names_by_remove_parent(self, object_instance: MO):
        if self._object_type_instance.primary:
            primary_values = []
            for tprm_id in self._object_type_instance.primary:
                for prm in object_instance.prms:
                    if prm.tprm_id == tprm_id:
                        primary_values.append(str(prm.value))

            name = NAME_DELIMITER.join(primary_values)
            query = select(MO).where(
                MO.id != object_instance.id,
                MO.name == name,
                MO.tmo_id == self._object_type_instance.id,
            )

            name_exist = self._session.exec(query).first()
            if name_exist:
                raise DuplicatedObjectName(
                    status_code=422,
                    detail="Unable to set p_id to null. Object names will be not unique.",
                )

            return name

        return str(object_instance.id)

    def _update_parent_id_attribute(
        self, object_to_update: dict, object_instance: MO
    ):
        if "p_id" in object_to_update and object_to_update["p_id"] is not None:
            query = select(MO).where(MO.id == object_to_update["p_id"])
            parent_object_instance = self._session.execute(query).scalar()

            self._check_parent_can_be_set(
                object_to_update=object_to_update,
                parent_object_instance=parent_object_instance,
            )

            new_object_name = self._update_object_names_by_changed_parent(
                parent_object_instance=parent_object_instance,
                object_instance=object_instance,
            )

        else:
            new_object_name = self._update_object_names_by_remove_parent(
                object_instance=object_instance
            )

        return new_object_name

    def execute(self) -> MO:
        self._update_object_data(
            object_instance=self._object_instance,
            object_to_update=self._request,
        )

        self._object_instance.modification_date = datetime.now(timezone.utc)
        self._session.add(self._object_instance)
        self._session.flush()

        updated_object = copy.deepcopy(self._object_instance)

        if self._autocommit:
            self._session.commit()

        return updated_object


class MassiveObjectUpdate:
    def __init__(
        self, objects_for_update: list[MassiveObjectsUpdate], session: Session
    ):
        self._objects_for_update = objects_for_update
        self._session = session

        self._object_instances_by_id = {}

    def _get_hashed_objects_by_id(self) -> dict[int, MO]:
        if self._object_instances_by_id:
            return self._object_instances_by_id

        object_instances = []
        requested_object_ids = {
            instance.object_id for instance in self._objects_for_update
        }

        for chunk in get_chunked_values_by_sqlalchemy_limit(
            values=requested_object_ids
        ):
            query = select(MO).where(MO.id.in_(chunk))
            object_instances.extend(
                self._session.execute(query).scalars().all()
            )
        self._object_instances_by_id = {
            object_instance.id: object_instance
            for object_instance in object_instances
        }
        return self._object_instances_by_id

    def check(self):
        self._get_hashed_objects_by_id()
        requested_object_ids = {
            instance.object_id for instance in self._objects_for_update
        }

        not_exists_objects = requested_object_ids.difference(
            set(self._object_instances_by_id.keys())
        )
        if not_exists_objects:
            raise ObjectNotExists(
                status_code=422,
                detail=f"There objects, which does not exist: {not_exists_objects}",
            )

        real_object_versions = {
            object_id: instance.version
            for object_id, instance in self._object_instances_by_id.items()
        }
        requested_object_versions = {
            instance.object_id: instance.data_for_update.version
            for instance in self._objects_for_update
        }

        not_valid_version_objects = [
            version
            for version in real_object_versions
            if real_object_versions[version]
            != requested_object_versions[version]
        ]

        if not_valid_version_objects:
            raise NotActualVersion(
                status_code=422,
                detail=f"Objects with ids: {not_valid_version_objects} must have another version!",
            )

    def execute(self):
        updated_objects = []
        for object_for_update in self._objects_for_update:
            db_object = self._object_instances_by_id[
                object_for_update.object_id
            ]
            object_for_update_instance = object_for_update.data_for_update

            if object_data_validation_when_update(
                db_object=db_object, upd_object=object_for_update_instance
            ):
                object_for_update_instance = object_for_update_instance.dict(
                    exclude_unset=True
                )

                task = UpdateObject(
                    session=self._session,
                    request=UpdateObjectRequest(
                        object_id=object_for_update.object_id,
                        **object_for_update_instance,
                    ),
                    autocommit=False,
                )
                task.check()

                updated_object = task.execute()

                updated_objects.append(updated_object)

        self._session.commit()
        return updated_objects


class DeleteObject(ObjectDBGetter):
    def __init__(self, session: Session, request: DeleteObjectRequest):
        self._session = session
        self._request = request
        super().__init__(session=session)

        self._object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

    def _validate_object_is_linked_to_another_object(self):
        query = select(TPRM.id).where(TPRM.val_type == "mo_link")
        tprms_with_val_type_mo_link = self._session.exec(query).all()

        if tprms_with_val_type_mo_link:
            stmt = f"SELECT id FROM tmo WHERE tmo.primary::jsonb @> '{tprms_with_val_type_mo_link}'::jsonb"
            tprms_with_val_type_mo_link_in_primary = self._session.execute(
                text(stmt)
            ).all()

            if tprms_with_val_type_mo_link_in_primary:
                query = select(PRM.tprm_id).where(
                    PRM.tprm_id.in_(tprms_with_val_type_mo_link),
                    PRM.value == str(id),
                )
                linked_mo = self._session.exec(query).all()

                if linked_mo:
                    raise CanNotDeletePrimaryObject(
                        status_code=422,
                        detail=f"Object with id: {self._request.object_id} can't be deleted, because he is primary "
                        f"part for other objects",
                    )

    def check(self):
        self._validate_object_is_linked_to_another_object()

    def _archive_object(self):
        self._object_instance.active = False
        self._session.add(self._object_instance)

        self._session.commit()
        self._session.refresh(self._object_instance)
        return self._object_instance

    def _delete_object_and_all_related_content(self):
        if self._request.delete_child:
            for child_object_instance in self._object_instance.children:
                proceed_object_delete(
                    session=self._session,
                    object_instance_to_delete=child_object_instance,
                )

        proceed_object_delete(
            session=self._session,
            object_instance_to_delete=self._object_instance,
        )

        self._session.commit()
        return {"ok": True}

    def execute(self):
        collapse_sequences_if_exist(
            session=self._session, object_instance=self._object_instance
        )

        if self._request.erase:
            return self._delete_object_and_all_related_content()

        return self._archive_object()


class CreateObjectWithParameters(
    ObjectDBGetter, ObjectTypeDBGetter, ParameterTypeDBGetter
):
    def __init__(self, session: Session, request: MOCreateWithParams):
        self._session = session
        self._request = request
        super().__init__(session=session)
        self._object_type_instance = self._get_object_type_instance_by_id(
            object_type_id=self._request.tmo_id
        )

    def _validate_sequence_parameter(
        self, parameter_type_instance: TPRM, parameter_to_create: PRMCreateByMO
    ):
        if parameter_type_instance.val_type == "sequence":
            query = select(PRM).where(
                PRM.tprm_id == parameter_type_instance.id,
                cast(PRM.value, Integer) >= parameter_to_create.value,
            )
            if parameter_type_instance.constraint:
                sequence_type = [
                    parameter_instance
                    for parameter_instance in self._request.params
                    if parameter_instance.tprm_id
                    == int(parameter_type_instance.constraint)
                ]
                sequence_type = sequence_type[0].value
                subquery = (
                    select(PRM.mo_id)
                    .where(
                        PRM.value == str(sequence_type),
                        PRM.tprm_id == int(parameter_type_instance.constraint),
                    )
                    .scalar_subquery()
                )
                query = query.where(PRM.mo_id.in_(subquery))

            query = query.execution_options(yield_per=100)
            params_to_update = (
                self._session.execute(query).scalars().partitions(100)
            )
            for chunk in params_to_update:  # type: list[PRM]
                for param_to_update in chunk:  # type: PRM
                    param_to_update.value = str(int(param_to_update.value) + 1)
                    self._session.add(param_to_update)

    @staticmethod
    def _set_new_latitude_attribute(
        new_parameter_instance: PRM,
        object_instance: MO,
        latitude_parameter_type_id: int,
    ):
        if new_parameter_instance.tprm_id == latitude_parameter_type_id:
            object_instance.latitude = float(new_parameter_instance.value)

    @staticmethod
    def _set_new_longitude_attribute(
        new_parameter_instance: PRM,
        object_instance: MO,
        longitude_parameter_type_id: int,
    ):
        if new_parameter_instance.tprm_id == longitude_parameter_type_id:
            object_instance.longitude = float(new_parameter_instance.value)

    @staticmethod
    def _set_new_status_attribute(
        new_parameter_instance: PRM,
        object_instance: MO,
        status_parameter_type_id: int,
    ):
        if new_parameter_instance.tprm_id == status_parameter_type_id:
            object_instance.status = new_parameter_instance.value

    def _set_object_attributes_by_parameter(
        self,
        new_parameter_instance: PRM,
        object_instance: MO,
        longitude_parameter_type_id: int,
        latitude_parameter_type_id: int,
        status_parameter_type_id: int,
    ):
        self._set_new_latitude_attribute(
            new_parameter_instance=new_parameter_instance,
            object_instance=object_instance,
            latitude_parameter_type_id=latitude_parameter_type_id,
        )

        self._set_new_longitude_attribute(
            new_parameter_instance=new_parameter_instance,
            object_instance=object_instance,
            longitude_parameter_type_id=longitude_parameter_type_id,
        )

        self._set_new_status_attribute(
            new_parameter_instance=new_parameter_instance,
            object_instance=object_instance,
            status_parameter_type_id=status_parameter_type_id,
        )

    def _process_formula_parameters(self, object_instance: MO):
        formula_parameter_types = self._get_parameter_type_instances_by_tmo_id(
            object_type_id=self._object_type_instance.id, val_type="formula"
        )
        for parameter_type_instance in formula_parameter_types:
            try:
                value = calculate_by_formula_new(
                    session=self._session,
                    param_type=parameter_type_instance,
                    object_instance=object_instance,
                )
            except ValueError as ex:
                raise NotValidFormulaParameterValue(
                    status_code=422, detail=f"{ex}"
                )

            new_parameter_instance = PRM(
                tprm_id=parameter_type_instance.id,
                mo_id=object_instance.id,
                value=value,
            )

            self._session.add(new_parameter_instance)

            try:
                self._session.flush()
                self._session.refresh(new_parameter_instance)
                self._session.flush()

            except IntegrityError as ex:
                if ex.orig.pgcode == "23505":
                    raise NotValidFormulaParameterValue(
                        status_code=409,
                        detail="You can't create formula TPRM. Unique violation.",
                    )
                continue

            self._session.refresh(new_parameter_instance)

        self._session.flush()

    def _process_sequence_parameters(self, object_instance: MO):
        sequence_parameter_types = self._get_parameter_type_instances_by_tmo_id(
            object_type_id=self._object_type_instance.id, val_type="sequence"
        )

        for seq_param in sequence_parameter_types:
            for param in self._request.params:  # type: PRMCreateByMO
                if param.tprm_id == seq_param.id:
                    break
            else:
                validate_object_parameters(
                    session=self._session,
                    parameter=PRMCreateByMO(value=None, tprm_id=seq_param.id),
                    object_instance=object_instance,
                    other_params=self._request.params,
                )
                sequence_type = None
                if seq_param.constraint:
                    query = select(PRM.value).where(
                        PRM.mo_id == object_instance.id,
                        PRM.tprm_id == int(seq_param.constraint),
                    )
                    sequence_type = self._session.execute(query).scalar()
                value = get_value_for_sequence(
                    self._session, seq_param, sequence_type
                )

                new_parameter_instance = PRM(
                    tprm_id=seq_param.id,
                    mo_id=object_instance.id,
                    value=value,
                )
                self._session.add(new_parameter_instance)

    def _process_primary_parameters(
        self,
        object_instance: MO,
        parent_object_instance: MO | None = None,
    ) -> NewObjectName:
        primary_values = []

        new_object_name = None

        if self._object_type_instance.primary:
            for primary_tprm_id in self._object_type_instance.primary:
                for parameter_to_create in object_instance.prms:
                    if parameter_to_create.tprm_id == primary_tprm_id:
                        query = select(TPRM.val_type).where(
                            TPRM.id == parameter_to_create.tprm_id
                        )
                        tprm_val_type = self._session.exec(query).first()

                        if tprm_val_type == "mo_link":
                            prm_value = self._get_object_instance_by_id(
                                object_id=int(parameter_to_create.value)
                            ).name
                        else:
                            prm_value = str(parameter_to_create.value)

                        primary_values.append(prm_value)
                        break

            is_set_parent_name = (
                not self._object_type_instance.global_uniqueness
                and self._request.p_id is not None
            )

            if is_set_parent_name:
                self._session.info["disable_security"] = True
                primary_values.insert(0, parent_object_instance.name)

            new_object_name = NAME_DELIMITER.join(primary_values)
            self._session.info["disable_security"] = True

            query = select(MO).where(
                MO.name == new_object_name, MO.tmo_id == self._request.tmo_id
            )
            duplicated_name = self._session.exec(query).first()
            if duplicated_name:
                raise DuplicatedObjectName(
                    status_code=409,
                    detail=f"Object with name '{new_object_name}' already exists.",
                )

        self._session.flush()
        return NewObjectName(
            primary_values=primary_values, new_object_name=new_object_name
        )

    @staticmethod
    def _process_new_object_name(
        object_instance: MO, object_name: NewObjectName
    ):
        if len(object_name.primary_values) == 0:
            object_instance.name = object_instance.id
            return object_instance.name

        object_instance.name = object_name.new_object_name
        return object_instance.name

    def _process_object_label_attribute(self, object_instance: MO):
        label_values = []
        for label_tprm_id in self._object_type_instance.label:
            for parameter_to_create in object_instance.prms:
                if parameter_to_create.tprm_id == label_tprm_id:
                    tprm_val_type = self._session.exec(
                        select(TPRM.val_type).where(
                            TPRM.id == parameter_to_create.tprm_id
                        )
                    ).first()

                    if tprm_val_type == "mo_link":
                        prm_value = self._session.exec(
                            select(MO.name).where(
                                MO.id == int(parameter_to_create.value)
                            )
                        ).first()
                    else:
                        prm_value = str(parameter_to_create.value)

                    label_values.append(prm_value)
                    break

        if label_values:
            label = NAME_DELIMITER.join(label_values)
            object_instance.label = label

        return label_values

    def _process_geometry_attributes(
        self, object_instance: MO, parent_object_instance: MO | None
    ):
        if (
            object_instance.point_a_id
            and object_instance.point_b_id
            and self._object_type_instance.geometry_type == GeometryType.line
            and not object_instance.geometry
        ):
            point_a_whereclauses = [MO.id == object_instance.point_a_id]
            point_b_whereclauses = [MO.id == object_instance.point_b_id]
            if self._object_type_instance.points_constraint_by_tmo:
                point_a_whereclauses.append(
                    MO.tmo_id.in_(
                        self._object_type_instance.points_constraint_by_tmo
                    )
                )
                point_b_whereclauses.append(
                    MO.tmo_id.in_(
                        self._object_type_instance.points_constraint_by_tmo
                    )
                )

            query = select(MO).where(*point_a_whereclauses)
            point_a = self._session.execute(query).scalar_one_or_none()

            query = select(MO).where(*point_b_whereclauses)
            point_b = self._session.execute(query).scalar_one_or_none()

            if not point_a:
                raise NotValidParameterValue(
                    status_code=422,
                    detail=f"You try to add point_a to MO with id {object_instance.point_a_id}, "
                    f"which is not match with object constraint",
                )
            if not point_b:
                raise NotValidParameterValue(
                    status_code=422,
                    detail=f"You try to add point_b MO with id {object_instance.point_b_id}, "
                    f"which is not match with object constraint",
                )

            object_instance.geometry = update_geometry(
                object_instance=object_instance,
                point_a=point_a,
                point_b=point_b,
            )

        # Set parent coords data
        if self._object_type_instance.inherit_location and self._request.p_id:
            if parent_object_instance.latitude:
                object_instance.latitude = parent_object_instance.latitude
            if parent_object_instance.longitude:
                object_instance.longitude = parent_object_instance.longitude
            if parent_object_instance.geometry:
                object_instance.geometry = parent_object_instance.geometry
            if parent_object_instance.point_a_id:
                object_instance.point_a_id = parent_object_instance.point_a_id
            if parent_object_instance.point_b_id:
                object_instance.point_b_id = parent_object_instance.point_b_id

    def _process_new_parameter_values(self, object_instance: MO):
        latitude_parameter_type_id = self._object_type_instance.latitude
        longitude_parameter_type_id = self._object_type_instance.longitude
        status_parameter_type_id = self._object_type_instance.status

        for parameter_to_create in self._request.params:
            validate_object_parameters(
                session=self._session,
                parameter=parameter_to_create,
                object_instance=object_instance,
                other_params=self._request.params,
            )

            self._session.info["disable_security"] = True

        created_parameters_list = []
        for parameter_to_create in self._request.params:
            parameter_type_instance = self._get_parameter_type_instance_by_id(
                parameter_type_id=parameter_to_create.tprm_id
            )

            self._validate_sequence_parameter(
                parameter_type_instance=parameter_type_instance,
                parameter_to_create=parameter_to_create,
            )

            if parameter_type_instance.multiple and isinstance(
                parameter_to_create.value, list
            ):
                parameter_to_create.value = pickle.dumps(
                    parameter_to_create.value
                ).hex()

            if parameter_type_instance.id in [
                latitude_parameter_type_id,
                longitude_parameter_type_id,
            ]:
                parameter_to_create.value = float(parameter_to_create.value)

            existing_parameter_instance = self._session.exec(
                select(PRM).where(
                    PRM.tprm_id == parameter_to_create.tprm_id,
                    PRM.mo_id == object_instance.id,
                )
            ).first()

            if existing_parameter_instance:
                existing_parameter_instance.value = parameter_to_create.value
                self._session.add(existing_parameter_instance)
                self._session.flush()
                self._session.refresh(existing_parameter_instance)

                new_parameter_instance = existing_parameter_instance
            else:
                new_parameter_instance = PRM(
                    tprm_id=parameter_to_create.tprm_id,
                    mo_id=object_instance.id,
                    value=parameter_to_create.value,
                )

                self._session.add(new_parameter_instance)
                self._session.flush()
                self._session.refresh(new_parameter_instance)

            self._set_object_attributes_by_parameter(
                new_parameter_instance=new_parameter_instance,
                object_instance=object_instance,
                latitude_parameter_type_id=latitude_parameter_type_id,
                longitude_parameter_type_id=longitude_parameter_type_id,
                status_parameter_type_id=status_parameter_type_id,
            )

            param_to_read = proceed_parameter_attributes(
                parameter_instance=new_parameter_instance
            )

            created_parameters_list.append(param_to_read)

        self._process_sequence_parameters(object_instance=object_instance)
        self._process_formula_parameters(object_instance=object_instance)

    def _process_object_attribute(
        self, parent_object_instance: MO | None, object_instance: MO
    ):
        object_name = self._process_primary_parameters(
            object_instance=object_instance,
            parent_object_instance=parent_object_instance,
        )

        self._process_new_object_name(
            object_instance=object_instance, object_name=object_name
        )

        self._process_object_label_attribute(object_instance=object_instance)

        self._process_geometry_attributes(
            object_instance=object_instance,
            parent_object_instance=parent_object_instance,
        )

    def _check_duplicated_parameter_type_id(self):
        parameter_type_ids = []
        for parameter in self._request.params:
            if parameter.tprm_id in parameter_type_ids:
                raise DuplicatedParameter(
                    status_code=422,
                    detail=f"Duplicated parameter type id {parameter.tprm_id} for object. "
                    "Its possible to create only one link for TPRM-MO",
                )
            parameter_type_ids.append(parameter.tprm_id)

    def check(self):
        check_if_all_required_params_passed(
            session=self._session, object=self._request
        )
        self._check_duplicated_parameter_type_id()

    def execute(self):
        object_instance = create_db_object(
            session=self._session,
            object_to_update=self._request,
            object_type=self._object_type_instance,
        )

        if self._request.p_id:
            parent_object_instance = self._get_object_instance_by_id(
                object_id=self._request.p_id
            )
        else:
            parent_object_instance = None

        self._session.add(object_instance)

        self._process_new_parameter_values(object_instance=object_instance)

        self._process_object_attribute(
            parent_object_instance=parent_object_instance,
            object_instance=object_instance,
        )

        self._session.add(object_instance)
        self._session.flush()
        self._session.commit()
        self._session.refresh(object_instance)

        object_to_read = get_object_with_parameters(
            session=self._session,
            db_object=object_instance,
            with_parameters=True,
        )

        return object_to_read


class GetChildObjectsWithProcessInstanceId(ObjectDBGetter):
    def __init__(
        self,
        request: GetChildObjectsWithProcessInstanceIdRequest,
        session: Session,
    ):
        super().__init__(session=session)

        self._request = request
        self._session = session
        self._parent_instance = self._get_object_instance_by_id(
            object_id=self._request.parent_object_id
        )

    @staticmethod
    async def _get_object_with_process_instances_id_from_zeebe(
        child_object_id: list[int],
    ) -> list[dict]:
        async with grpc.aio.insecure_channel(
            f"{ZEEBE_GRPC_HOST}:{ZEEBE_GRPC_PORT}"
        ) as channel:
            stub = zeebe_severity_pb2_grpc.SeverityStub(channel)
            message = (
                zeebe_severity_pb2.GetChildObjectsWithProcessInstanceRequest(
                    object_id=child_object_id
                )
            )
            response_async_generator = stub.GetChildObjectsWithProcessInstance(
                message
            )

            async for item in response_async_generator:
                grpc_data_response = json_format.MessageToDict(
                    item,
                    including_default_value_fields=True,
                    preserving_proto_field_name=True,
                )
                return grpc_data_response["objects_with_process_instance_id"]

    def _get_child_object_ids(self):
        return [
            child_object.id for child_object in self._parent_instance.children
        ]

    async def execute(self):
        child_object_id = self._get_child_object_ids()

        object_with_process_instance_ids = (
            await self._get_object_with_process_instances_id_from_zeebe(
                child_object_id=child_object_id
            )
        )

        return object_with_process_instance_ids


class GetSiteFiber:
    def __init__(self, request: GetSiteFiberRequest, session: Session):
        self._request = request
        self._session = session

    def execute(self):
        query = select(MO).where(MO.point_a_id == self._request.point_a_id)
        object_instance = self._session.exec(query).first()
        if object_instance:
            return object_instance

        raise PointInstanceNotExists(
            status_code=404,
            detail=f"Fiber with point_a_id = {self._request.point_a_id} does not exist",
        )


class GetObjectsByNames:
    def __init__(self, request: GetObjectsByNamesRequest, session: Session):
        self._request = request
        self._session = session

    def _get_filter_conditions(self):
        conditions = []
        if self._request.tmo_id:
            conditions.append(MO.tmo_id == self._request.tmo_id)

        if self._request.objects_names:
            conditions.append(
                or_(
                    MO.name.ilike(f"%{name}%")
                    for name in self._request.objects_names
                )
            )

        return conditions

    def execute(self):
        filters_by_request = self._get_filter_conditions()

        if filters_by_request:
            query_to_get_instances = (
                select(MO)
                .where(and_(*filters_by_request))
                .order_by(MO.id)
                .limit(self._request.limit)
                .offset(self._request.offset)
            )
            query_to_get_quantity = (
                select(func.count())
                .select_from(MO)
                .where(and_(*filters_by_request))
            )

            object_instances = (
                self._session.execute(query_to_get_instances).scalars().all()
            )
            objects_quantity = self._session.scalar(query_to_get_quantity)

            response_data = get_parameters_for_object_by_object_query(
                session=self._session,
                db_mos=object_instances,
                mos_ids=[],
                identifiers_instead_of_values=self._request.identifiers_instead_of_values,
            )
            return GetObjectsByNamesResponse(
                data=response_data, total=objects_quantity
            )

        response_data = get_objects_with_parameters(
            session=self._session,
            limit=self._request.limit,
            order_by_rule="desc",
            offset=self._request.offset,
            identifiers_instead_of_values=self._request.identifiers_instead_of_values,
        )
        query = select(func.count()).select_from(MO)
        objects_quantity = self._session.exec(query).first()

        return GetObjectsByNamesResponse(
            data=response_data, total=objects_quantity
        )


class GetObjectWithGroupedParameters(ObjectDBGetter):
    def __init__(
        self, session: Session, request: GetObjectWithGroupedParametersRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request
        self._object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

    def _get_grouped_parameters(
        self, parameter_instances: list[PRM]
    ) -> dict[str, list[GroupedParam]]:
        query = select(TPRM).where(TPRM.tmo_id == self._object_instance.tmo_id)
        param_types = self._session.exec(query).all()

        grouped_filled_params = {
            parameter_type_id: list(params)
            for parameter_type_id, params in itertools.groupby(
                parameter_instances, key=lambda x: x.tprm_id
            )
        }

        grouped_result = defaultdict(list)
        return get_grouped_params(
            param_types=param_types,
            grouped_filled_params=grouped_filled_params,
            only_filled=self._request.only_filled,
            grouped_result=grouped_result,
            db_object=self._object_instance,
        )

    @staticmethod
    def _format_parameters_for_response(
        grouped_parameters: dict[str, list[GroupedParam]],
    ) -> list[ResponseGroupedParams]:
        sorted_result = dict()
        for key in sorted(
            grouped_parameters.keys(), key=lambda x: x.strip() if x else ""
        ):
            sorted_result[key] = sorted(
                grouped_parameters[key], key=lambda x: x.name
            )

        return [
            ResponseGroupedParams(name=key, params=values)
            for key, values in sorted_result.items()
        ]

    def execute(self):
        object_to_read = get_object_with_parameters(
            session=self._session,
            db_object=self._object_instance,
            with_parameters=True,
        )

        grouped_parameters = self._get_grouped_parameters(
            parameter_instances=object_to_read["params"]
        )

        return self._format_parameters_for_response(
            grouped_parameters=grouped_parameters
        )


class GetLinkedObjectsByParametersLink(
    ObjectDBGetter, ParameterTypeDBGetter, ParameterDBGetter
):
    def __init__(
        self, request: GetLinkedObjectsByParametersLinkRequest, session: Session
    ):
        super().__init__(session=session)
        self._request = request
        self._session = session

    @staticmethod
    def _get_linked_parameter_instances_by_id_from_multiple(
        parameter_instances_by_id: dict[int, PRM],
    ) -> set[int]:
        linked_parameters_ids = set()
        for (
            parameter_instance_id,
            parameter_instance,
        ) in parameter_instances_by_id.items():
            linked_parameters_ids.update(
                set(decode_pickle_data(parameter_instance.value))
            )

        return linked_parameters_ids

    def _get_linked_object_instance(
        self, cache_object_instances: dict[int, MO], parameter_instance: PRM
    ) -> MO:
        linked_object_instance = cache_object_instances.get(
            parameter_instance.mo_id
        )
        if linked_object_instance:
            return linked_object_instance

        linked_object_instance = self._get_object_instance_by_id(
            object_id=parameter_instance.mo_id
        )
        cache_object_instances[linked_object_instance.id] = (
            linked_object_instance
        )
        return linked_object_instance

    def _get_linked_parameter_type_instance(
        self,
        cache_parameter_type_instances: dict[int, TPRM],
        parameter_instance: PRM,
    ) -> TPRM:
        linked_parameter_type_instance = cache_parameter_type_instances.get(
            parameter_instance.tprm_id
        )
        if linked_parameter_type_instance:
            return linked_parameter_type_instance

        linked_parameter_type_instance = (
            self._get_parameter_type_instance_by_id(
                parameter_type_id=parameter_instance.tprm_id
            )
        )
        cache_parameter_type_instances[linked_parameter_type_instance.id] = (
            linked_parameter_type_instance
        )
        return linked_parameter_type_instance

    def _get_service_data_for_parameter(
        self,
        cache_object_instances: dict[int, MO],
        cache_parameter_type_instances: dict[int, TPRM],
        parameter_instance: PRM,
    ):
        linked_object_instance = self._get_linked_object_instance(
            cache_object_instances=cache_object_instances,
            parameter_instance=parameter_instance,
        )
        linked_parameter_type_instance = (
            self._get_linked_parameter_type_instance(
                cache_parameter_type_instances=cache_parameter_type_instances,
                parameter_instance=parameter_instance,
            )
        )

        return LinkedParameterInstances(
            object_instance=linked_object_instance,
            parameter_type_instance=linked_parameter_type_instance,
        )

    def _process_multiple_values(self, parameter_instances: list[PRM]):
        processed_parameters = []
        cache_object_instances = {}
        cache_parameter_type_instances = {}

        parameter_instances_by_id = {
            parameter_instance.id: parameter_instance
            for parameter_instance in parameter_instances
        }

        linked_parameter_ids = (
            self._get_linked_parameter_instances_by_id_from_multiple(
                parameter_instances_by_id=parameter_instances_by_id
            )
        )
        linked_parameter_instances_by_id = self._get_parameters_by_ids(
            parameter_ids=linked_parameter_ids
        )

        for parameter_instance in parameter_instances:
            linked_parameters_ids: list = decode_pickle_data(
                parameter_instance.value
            )

            for linked_parameter_ids in linked_parameters_ids:
                linked_parameter_instance = (
                    linked_parameter_instances_by_id.get(linked_parameter_ids)
                )

                # if we don't need to check and prm exists
                if linked_parameter_instance:
                    linked_parameter_data = self._get_service_data_for_parameter(
                        cache_object_instances=cache_object_instances,
                        cache_parameter_type_instances=cache_parameter_type_instances,
                        parameter_instance=linked_parameter_instance,
                    )

                    values = [parameter_instance.value]
                    if linked_parameter_data.parameter_type_instance.multiple:
                        values: list = decode_pickle_data(
                            parameter_instance.value
                        )

                    if values:
                        processed_parameters.append(
                            ParameterDataWithObject(
                                prm_id=parameter_instance.id,
                                mo_id=parameter_instance.mo_id,
                                mo_name=linked_parameter_data.object_instance.name,
                                prm_value=values,
                            ).dict()
                        )

        return processed_parameters

    def _process_single_values(self, parameter_instances: list[PRM]):
        processed_parameters = []

        parameter_ids = [
            int(parameter_instance.value)
            for parameter_instance in parameter_instances
        ]
        parameter_instances_by_id = self._get_parameters_by_ids(
            parameter_ids=parameter_ids
        )

        parameter_type_ids = [
            parameter.tprm_id
            for parameter in parameter_instances_by_id.values()
        ]
        parameter_type_instances_by_id = self._get_parameters_type_by_ids(
            parameter_type_ids=parameter_type_ids
        )

        for parameter_instance in parameter_instances:
            current_prm = parameter_instances_by_id.get(
                int(parameter_instance.value)
            )

            if current_prm:
                parameter_type_instance = parameter_type_instances_by_id.get(
                    current_prm.tprm_id
                )

                values = [current_prm.value]
                if parameter_type_instance.multiple:
                    values: list = decode_pickle_data(current_prm.value)

                if values:
                    object_instance = self._get_object_instance_by_id(
                        object_id=current_prm.mo_id
                    )
                    processed_parameters.append(
                        ParameterDataWithObject(
                            prm_id=current_prm.id,
                            mo_id=current_prm.mo_id,
                            mo_name=object_instance.name,
                            prm_value=values,
                        ).dict()
                    )
        return processed_parameters

    def execute(self):
        # TPRM can be only prm_link type, because, it stores prm id in value
        query = select(TPRM).where(
            TPRM.id == self._request.parameter_type_id,
            TPRM.val_type == "prm_link",
        )
        parameter_type_instance = self._session.exec(query).first()

        if parameter_type_instance:
            # get all prms of it tprm to check all prm values of prm of current TPRM value
            query = (
                select(PRM)
                .where(PRM.tprm_id == self._request.parameter_type_id)
                .limit(self._request.limit)
                .offset(self._request.offset)
            )
            parameter_instances = self._session.execute(query).scalars().all()

            # if current TPRM is multiple -- it store pickle data and inside list there are prm ids
            if parameter_type_instance.multiple:
                return self._process_multiple_values(
                    parameter_instances=parameter_instances
                )

            return self._process_single_values(
                parameter_instances=parameter_instances
            )

        return []


class MassiveObjectDelete:
    def __init__(self, session: Session, request: MassiveObjectDeleteRequest):
        self._session = session
        self._request = request

    @staticmethod
    def _check_object_exists(
        requested_object_ids: set[int], object_instances: list[MO]
    ):
        object_instance_ids = {
            object_instance.id for object_instance in object_instances
        }
        not_exists_objects = requested_object_ids.difference(
            object_instance_ids
        )
        if not_exists_objects:
            raise ObjectNotExists(
                status_code=422,
                detail=f"There objects, which does not exist: {not_exists_objects}",
            )

    def _permanent_object_instances_delete(
        self, requested_object_ids: set[int], object_instances: list[MO]
    ):
        check_mo_is_part_of_other_mo_name(
            session=self._session, object_instance_ids=requested_object_ids
        )

        if self._request.delete_children:
            all_children = []
            for object_instance in object_instances:
                all_children.extend(object_instance.children)

            children_object_type_ids = {
                str(child.tmo_id) for child in all_children
            }
            proceed_object_list_delete(
                session=self._session,
                object_instances=all_children,
                object_type_ids=children_object_type_ids,
            )

        object_type_ids = {
            str(object_instance.tmo_id) for object_instance in object_instances
        }
        proceed_object_list_delete(
            session=self._session,
            object_instances=object_instances,
            object_type_ids=object_type_ids,
        )

    def _deactivate_object_instances(self, object_instances: list[MO]):
        for object_instance in object_instances:
            object_instance.active = False
            object_instance.version += 1
            self._session.add(object_instance)

    def execute(self):
        object_instances = []
        requested_object_ids = set(self._request.mo_ids)

        for chunk in get_chunked_values_by_sqlalchemy_limit(
            values=requested_object_ids
        ):
            query = select(MO).where(MO.id.in_(chunk))
            object_instances.extend(
                self._session.execute(query).scalars().all()
            )

        self._check_object_exists(
            requested_object_ids=requested_object_ids,
            object_instances=object_instances,
        )

        if self._request.erase:
            self._permanent_object_instances_delete(
                requested_object_ids=requested_object_ids,
                object_instances=object_instances,
            )

        else:
            self._deactivate_object_instances(object_instances=object_instances)

        self._session.commit()
        return {"status": "Objects were successfully deleted"}


class GetAllParentsForObject(ObjectDBGetter):
    def __init__(
        self, session: Session, request: GetAllParentsForObjectRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        self._get_object_instance_by_id(object_id=self._request.object_id)

        object_instances_ids = get_items_recursive_up(
            session=self._session,
            main_table=MO,
            instance_id=self._request.object_id,
        )

        if object_instances_ids:
            query = select(MO).where(MO.id.in_(object_instances_ids))
            object_instances = self._session.execute(query).scalars().all()
            return sorted(
                object_instances,
                key=lambda object_instance: object_instances_ids.index(
                    object_instance.id
                ),
            )

        return []


class GetAllParentsForObjectMassive(ObjectDBGetter):
    def __init__(
        self, session: Session, request: GetAllParentsForObjectMassiveRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        query = select(MO.id).where(MO.id.in_(self._request.object_ids))
        object_instance_ids: list[MO] = (
            self._session.execute(query).scalars().all()
        )

        linked_object_ids_by_requested = {}
        for object_id in object_instance_ids:
            linked_object_ids = get_items_recursive_up(
                session=self._session, main_table=MO, instance_id=object_id
            )
            linked_object_ids_by_requested[object_id] = linked_object_ids

        all_linked_object_ids = set()
        for _, linked_object_ids in linked_object_ids_by_requested.items():
            all_linked_object_ids.update(set(linked_object_ids))

        all_linked_object_ids = list(all_linked_object_ids)
        query = select(MO).where(MO.id.in_(all_linked_object_ids))
        linked_object_instances = self._session.execute(query).scalars().all()
        linked_object_instances_by_id = {
            object_instance.id: object_instance
            for object_instance in linked_object_instances
        }

        response = {}
        if all_linked_object_ids:
            for (
                required_object_id,
                linked_object_ids,
            ) in linked_object_ids_by_requested.items():
                object_instances = [
                    linked_object_instances_by_id[object_id]
                    for object_id in linked_object_ids
                ]

                response[required_object_id] = sorted(
                    object_instances,
                    key=lambda object_instance: all_linked_object_ids.index(
                        object_instance.id
                    ),
                )

        return response


class GetParentInheritLocation(ObjectDBGetter, ObjectTypeDBGetter):
    def __init__(
        self, session: Session, request: GetParentInheritLocationRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

        object_type_instance = self._get_object_type_instance_by_id(
            object_type_id=object_instance.tmo_id
        )

        object_type_instance, object_instance = find_deep_parent(
            session=self._session,
            object_instance=object_instance,
            object_type_instance=object_type_instance,
            from_parent=True,
        )

        if object_type_instance and object_instance:
            return MOInheritParent(
                parent_mo=object_instance,
                tprm_latitude=object_type_instance.latitude,
                tprm_longitude=object_type_instance.longitude,
            )

        return MOInheritParent(
            parent_mo=None, tprm_latitude=None, tprm_longitude=None
        )


class GetObjectsByParameter(ParameterTypeDBGetter):
    def __init__(self, session: Session, request: GetObjectsByParameterRequest):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def _get_parameters_data(self) -> list[dict]:
        conditions = [PRM.tprm_id == self._request.parameter_type_id]

        if self._request.value:
            conditions.append(
                func.lower(PRM.value).ilike(f"%{self._request.value}%")
            )

        prm_with_mos_query = (
            select(PRM, MO)
            .join(MO)
            .where(*conditions)
            .limit(self._request.limit)
            .offset(self._request.offset)
        )
        parameter_with_object_instances = (
            self._session.execute(prm_with_mos_query).scalars().all()
        )

        return [
            ParameterDataWithObject(
                mo_name=parameter_with_object_instance.mo.name,
                mo_id=parameter_with_object_instance.mo.id,
                prm_value=parameter_with_object_instance.value,
                prm_id=parameter_with_object_instance.id,
            ).dict()
            for parameter_with_object_instance in parameter_with_object_instances
        ]

    def _get_parameter_quantity(self, parameter_instances: list[dict]) -> int:
        if self._request.value:
            return len(parameter_instances)

        query = (
            select(func.count())
            .select_from(PRM)
            .where(PRM.tprm_id == self._request.parameter_type_id)
        )
        return self._session.exec(query).first()

    def execute(self):
        self._get_parameter_type_instance_by_id(
            parameter_type_id=self._request.parameter_type_id
        )

        parameter_instances = self._get_parameters_data()
        parameters_quantity = self._get_parameter_quantity(
            parameter_instances=parameter_instances
        )

        return GetObjectsByParameterResponse(
            data=parameter_instances, total=parameters_quantity
        )


class ReadObjectByObjectTypes(ObjectDBGetter, ObjectTypeDBGetter):
    def __init__(
        self, session: Session, request: GetObjectsByObjectTypeRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        # get existing tmo
        all_tmo_ids = self._request.object_type_ids[:]

        if self._request.show_objects_of_children_object_types:
            child_objects = recursive_find_children_all_children_tmo(
                tmo_ids=self._request.object_type_ids, session=self._session
            )

            all_tmo_ids = self._request.object_type_ids + child_objects

        coord_conditions = get_conditions_for_coords(
            outer_box_longitude_min=self._request.outer_box_longitude_min,
            outer_box_longitude_max=self._request.outer_box_longitude_max,
            outer_box_latitude_min=self._request.outer_box_latitude_min,
            outer_box_latitude_max=self._request.outer_box_latitude_max,
            inner_box_longitude_min=self._request.inner_box_longitude_min,
            inner_box_longitude_max=self._request.inner_box_longitude_max,
            inner_box_latitude_min=self._request.inner_box_latitude_min,
            inner_box_latitude_max=self._request.inner_box_latitude_max,
        )

        stmt = (
            select(MO.id)
            .where(MO.tmo_id.in_(all_tmo_ids), *coord_conditions)
            .distinct()
        )
        object_ids = self._session.execute(stmt).scalars().all()

        objects_to_read = get_objects_with_parameters(
            session=self._session,
            limit=self._request.limit,
            offset=self._request.offset,
            mos_ids=object_ids,
            returnable=not self._request.with_parameters,
            active=self._request.active,
            identifiers_instead_of_values=self._request.identifiers_instead_of_values,
        )
        results_length = count_objects(
            session=self._session,
            mos_ids=object_ids,
            active=self._request.active,
        )

        stmt = select(TMO.id, TMO.icon).where(TMO.id.in_(all_tmo_ids))
        object_id_with_icon = self._session.exec(stmt).all()
        return GetObjectsByObjectTypeResponse(
            object_types=[
                {"id": object_type_id, "icon": icon}
                for object_type_id, icon in object_id_with_icon
            ],
            objects=objects_to_read,
            results_length=results_length,
        )


class GetObjectWithParameters(ObjectDBGetter):
    def __init__(
        self, session: Session, request: GetObjectWithParametersRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        object_instance = self._get_object_instance_by_id(
            object_id=self._request.object_id
        )

        object_to_read = get_object_with_parameters(
            session=self._session,
            db_object=object_instance,
            with_parameters=self._request.with_parameters,
        )
        return object_to_read


class RebuildGeometry(ObjectTypeDBGetter):
    def __init__(
        self, session: sqlmodel_Session, request: RebuildGeometryRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    async def execute(self):
        self._get_object_type_instance_by_id(
            object_type_id=self._request.object_type_id
        )

        output: io.StringIO = reconstruct_geometry(
            session=self._session,
            tmo_id=self._request.object_type_id,
            correct=self._request.correct,
        )
        response = StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment;filename=dataset.csv",
                "Access-Control-Expose-Headers": "Content-Disposition",
            },
        )
        return response


class UpdateObjectNamesWithNullNames:
    def __init__(self, session: Session):
        self._session = session

    def execute(self):
        # get object instance with name equals None
        query = select(MO).where(MO.name.is_(None))
        objects_with_null_objects: list[MO] = (
            self._session.execute(query).scalars().all()
        )
        object_type_ids: list[int] = [
            object_instance.tmo_id
            for object_instance in objects_with_null_objects
        ]

        # get object types of specific objects
        query = select(TMO).where(TMO.id.in_(object_type_ids))
        object_type_instances: list[TMO] = (
            self._session.execute(query).scalars().all()
        )
        object_type_instance_by_id: dict[int, TMO] = {
            object_type_instance.id: object_type_instance
            for object_type_instance in object_type_instances
        }

        updated_list = []
        error_list = []

        # update object name according to object type primary
        for object_instance in objects_with_null_objects:
            try:
                object_type_instance = object_type_instance_by_id.get(
                    object_instance.tmo_id
                )

                new_object_name_parts = []
                if object_type_instance.primary:
                    for parameter_type_id in object_type_instance.primary:
                        parameter_type_instance = self._session.get(
                            TPRM, parameter_type_id
                        )

                        query = select(PRM).where(
                            PRM.tprm_id == parameter_type_instance.id,
                            PRM.mo_id == object_instance.id,
                        )
                        parameter: PRM = self._session.execute(query).scalar()
                        match parameter_type_instance.val_type:
                            case "mo_link":
                                value = self._session.get(
                                    MO, int(parameter.value)
                                ).name

                            case "prm_link":
                                value = self._session.get(
                                    PRM, int(parameter.value)
                                ).value

                            case _:
                                value = parameter.value

                        new_object_name_parts.append(value)
                    object_instance.name = NAME_DELIMITER.join(
                        new_object_name_parts
                    )

                else:
                    object_instance.name = str(object_instance.id)

            except Exception as e:
                print(e)
                error_list.append(object_instance)

            else:
                updated_list.append(
                    [
                        object_instance.id,
                        object_instance.name,
                        object_instance.tmo_id,
                    ]
                )
                self._session.add(object_instance)

        pprint(f"updated_list : {updated_list}")
        pprint(f"error_list : {error_list}")
        self._session.commit()
