import base64
import json
from typing import Iterator

import grpc
from fastapi import HTTPException
from google.protobuf import json_format
from sqlalchemy import and_, delete
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select
from psycopg2.errors import UniqueViolation

from database import engine
from functions.db_functions.db_create import create_db_object_type
from routers.object_type_router.schemas import TMOCreate
from routers.parameter_type_router.schemas import TPRMCreate
from services.grpc_service.grpc_utils import (
    batch_import,
    batch_export,
    create_tprm,
)
from services.grpc_service.proto_files.airflow.files.airflow_manager_pb2 import (
    ResponseBatchImport,
    RequestBatchImport,
    ResponseCreateTMOOrGetInfo,
    RequestCreateTMOOrGetInfo,
    RequestCreateTPRMsForTMO,
    ResponseCreateTPRMsForTMO,
    RequestGetTMOName,
    TPRMNameToId,
    RequestBatchExport,
    ResponseGetTMOName,
    ResponseBatchExport,
    RequestGetTPRMNamesByIds,
    ResponseGetTPRMNamesByIds,
    RequestGetRequiredFields,
    ResponseGetRequiredFields,
    RequestGetMOAttrsAndTPRMs,
    ResponseGetMOAttrsAndTPRMs,
    RequestGetTMOLocations,
    ResponseGetTMOLocations,
    RequestDeleteAllObjects,
    ResponseDeleteAllObjects,
)
from services.grpc_service.proto_files.airflow.files.airflow_manager_pb2_grpc import (
    AirflowManagerServicer,
)

from models import TMO, TPRM, MO
from routers.batch_router.schemas import ExportFileTypes


class AirflowManager(AirflowManagerServicer):
    def BatchImport(
        self,
        request_iterator: Iterator[RequestBatchImport],
        context: grpc.ServicerContext,
    ) -> ResponseBatchImport:
        print("GRPC BATCH IMPORT")
        encoded = ""
        first = True
        tmo_id = None
        for req in request_iterator:
            if first:
                tmo_id = req.tmo_id
            encoded += req.content

        content = base64.b64decode(encoded.encode("utf-8"))
        print("grabbed")
        with Session(engine) as session:
            try:
                batch_import(session=session, file_data=content, tmo_id=tmo_id)
            except HTTPException as exc:
                return ResponseBatchImport(status="ERROR", message=exc.detail)

        print("ready for response")
        return ResponseBatchImport(status="OK")

    def BatchExport(
        self, request: RequestBatchExport, context: grpc.ServicerContext
    ) -> ResponseBatchExport:
        if request.file_type not in ExportFileTypes.values():
            yield ResponseBatchExport(
                status="ERROR", message="Unsupported file_type!"
            )
        try:
            with Session(engine) as session:
                for part in batch_export(
                    session=session,
                    tmo_id=request.tmo_id,
                    file_type=request.file_type,
                ):
                    yield ResponseBatchExport(status="SUCCESS", content=part)
        except HTTPException as exc:
            yield ResponseBatchExport(status="ERROR", message=exc.detail)

    def CreateTMOorGetInfo(
        self, request: RequestCreateTMOOrGetInfo, context: grpc.ServicerContext
    ) -> ResponseCreateTMOOrGetInfo:
        with Session(engine) as session:
            msg_as_dict = json_format.MessageToDict(
                request,
                including_default_value_fields=True,
                preserving_proto_field_name=True,
            )
            session.info["disable_security"] = True
            tmo_exist = session.exec(
                select(TMO).where(TMO.name == msg_as_dict["name"])
            ).first()
            if tmo_exist:
                return ResponseCreateTMOOrGetInfo(
                    status="SUCCESS", tmo_id=tmo_exist.id, name=tmo_exist.name
                )
            try:
                db_object_type = create_db_object_type(
                    session=session, object_type=TMOCreate(**msg_as_dict)
                )
            except HTTPException as exc:
                return ResponseCreateTMOOrGetInfo(
                    status="ERROR", message=exc.detail
                )
            session.add(db_object_type)
            session.flush([db_object_type])
            session.refresh(db_object_type)
            session.commit()
            session.refresh(db_object_type)

            return ResponseCreateTMOOrGetInfo(
                status="SUCCESS",
                tmo_id=db_object_type.id,
                name=db_object_type.name,
            )

    def CreateTPRMsForTMO(
        self, request: RequestCreateTPRMsForTMO, context: grpc.ServicerContext
    ) -> ResponseCreateTPRMsForTMO:
        result = {}
        with Session(engine) as session:
            msg_as_dict = json_format.MessageToDict(
                request,
                including_default_value_fields=True,
                preserving_proto_field_name=True,
            )
            for item in msg_as_dict["tprms"]:
                item["tmo_id"] = request.tmo_id
                tprm = TPRMCreate(**item)
                try:
                    result[tprm.name] = create_tprm(session, tprm)
                except HTTPException as exc:
                    return ResponseCreateTPRMsForTMO(
                        status="ERROR", message=exc.detail
                    )
                except IntegrityError as exc:
                    session.rollback()
                    if isinstance(exc.__cause__, UniqueViolation):
                        query = select(TPRM.id).where(
                            and_(
                                TPRM.tmo_id == request.tmo_id,
                                TPRM.name == tprm.name,
                            )
                        )
                        response = session.execute(query)
                        result[tprm.name] = response.scalars().first()
                    else:
                        return ResponseCreateTPRMsForTMO(
                            status="ERROR", message=str(exc)
                        )

            session.commit()

        return ResponseCreateTPRMsForTMO(
            status="SUCCESS", tprms=TPRMNameToId(mapper=result)
        )

    def DeleteAllObjectsInTMO(
        self, request: RequestDeleteAllObjects, context: grpc.ServicerContext
    ):
        with Session(engine) as session:
            query = (
                delete(MO).where(MO.tmo_id == request.tmo_id).returning(MO.id)
            )
            response = session.execute(query)
            if len(response.scalars().all()) == 0:
                query = select(TMO).where(TMO.id == request.tmo_id)
                response = session.execute(query)
                if not response.fetchone():
                    return ResponseDeleteAllObjects(
                        status="ERROR", message="TMO doesn't exist!"
                    )
            session.commit()

        return ResponseDeleteAllObjects(status="SUCCESS")

    def GetTMOName(
        self, request: RequestGetTMOName, context: grpc.ServicerContext
    ):
        with Session(engine) as session:
            query = select(TMO.name).where(TMO.id == request.tmo_id)
            response = session.execute(query)
            tmo_name = response.scalars().one_or_none()
            if tmo_name:
                return ResponseGetTMOName(name=tmo_name)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"TMO with id={request.tmo_id} not found!")
                return context

    def GetTPRMNamesByIds(
        self, request: RequestGetTPRMNamesByIds, context: grpc.ServicerContext
    ) -> ResponseGetTPRMNamesByIds:
        with Session(engine) as session:
            query = select(TPRM.id, TPRM.name).where(
                TPRM.id.in_(request.tprm_ids)
            )
            response = session.execute(query)
            response = {res[0]: res[1] for res in response.fetchall()}

        # if any column is missing
        if len(response.keys()) != len(request.tprm_ids):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"{set(request.tprm_ids).difference(response.keys())} columns not found!"
            )
            return context

        return ResponseGetTPRMNamesByIds(mapper=response)

    def GetRequiredFields(
        self, request: RequestGetRequiredFields, context: grpc.ServicerContext
    ) -> ResponseGetRequiredFields:
        with Session(engine) as session:
            query = select(TPRM.id, TPRM.name).where(
                and_(TPRM.tmo_id == request.tmo_id, TPRM.required.is_(True))
            )
            response = session.execute(query)

            return ResponseGetRequiredFields(
                fields={res[0]: res[1] for res in response.all()}
            )

    def GetMOAttrsAndTPRMs(
        self, request: RequestGetMOAttrsAndTPRMs, context: grpc.ServicerContext
    ) -> ResponseGetMOAttrsAndTPRMs:
        with Session(engine) as session:
            result = ["id", "name", "parent_name"]
            query = select(TPRM.name).where(TPRM.tmo_id == request.tmo_id)
            response = session.exec(query).all()
            result.extend(response)
            return ResponseGetMOAttrsAndTPRMs(columns=result)

    def GetTMOLocations(
        self,
        request: RequestGetTMOLocations,
        context: grpc.ServicerContext,
    ) -> ResponseGetTMOLocations:
        with Session(engine) as session:
            query = select(TMO.id).where(TMO.id == request.tmo_id)
            response = session.execute(query).scalar()
            if response is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("TMO with given id does not exist!")
                return context

            query = (
                select(MO.name, MO.geometry)
                .where(MO.tmo_id == request.tmo_id)
                .execution_options(yield_per=100)
            )
            chunks = session.execute(query)
            for chunk in chunks.mappings().partitions(100):
                for item in chunk:
                    yield ResponseGetTMOLocations(
                        name=item["name"], geometry=json.dumps(item["geometry"])
                    )
