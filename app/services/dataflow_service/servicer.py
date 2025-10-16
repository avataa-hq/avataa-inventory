import json

import grpc
from sqlalchemy import engine, select
from sqlmodel import Session

from functions.db_functions.db_read import get_objects_with_parameters
from services.grpc_service.proto_files.dataflow.files import (
    dataflow_manager_pb2,
)
from models import TPRM
from services.grpc_service.proto_files.dataflow.files.dataflow_manager_pb2_grpc import (
    DataflowManagerServicer,
)


class DataflowManager(DataflowManagerServicer):
    def GetTPRMNamesOfTMO(
        self,
        request: dataflow_manager_pb2.RequestGetTPRMNamesOfTMO,
        context: grpc.ServicerContext,
    ) -> dataflow_manager_pb2.ResponseGetTPRMNamesOfTMO:
        with Session(engine) as session:
            query = select(TPRM.name).where(TPRM.tmo_id == request.tmo_id)
            params_names = session.exec(query)
            return dataflow_manager_pb2.ResponseGetTPRMNamesOfTMO(
                column=params_names.all()
            )

    def GetObjectsWithParams(
        self,
        request: dataflow_manager_pb2.RequestGetObjectsWithParams,
        context: grpc.ServicerContext,
    ) -> dataflow_manager_pb2.ResponseGetObjectsWithParams:
        if request.tprm_names:
            request.tprm_names.extend(["tmo_id"])
        with Session(engine) as session:
            result = get_objects_with_parameters(
                session=session,
                limit=request.limit,
                offset=request.offset if request.offset else None,
                object_type_id=request.tmo_id,
                returnable=False,
                active=True,
            )
            try:
                ids = set()
                for param in result[0]["params"]:
                    ids.add(param.tprm_id)
                response = session.execute(
                    select(TPRM.id, TPRM.name).where(TPRM.id.in_(ids))
                )
                tprm_id_to_names = {
                    res[0]: res[1] for res in response.fetchall()
                }
                if not request.tprm_names:
                    request.tprm_names.extend(["tmo_id"])
                    request.tprm_names.extend(tprm_id_to_names.values())

            except IndexError:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Objects not found!")
                return context

            for res in result:
                params = res.pop("params")

                for param in params:
                    res[tprm_id_to_names[param.tprm_id]] = param.value

                res = {k: v for k, v in res.items() if k in request.tprm_names}
                yield dataflow_manager_pb2.ResponseGetObjectsWithParams(
                    data=json.dumps(res)
                )

    def GetTPRMNameToTypeMapper(
        self,
        request: dataflow_manager_pb2.RequestGetTPRMNameToTypeMapper,
        context: grpc.ServicerContext,
    ) -> dataflow_manager_pb2.ResponseGetTPRMNameToTypeMapper:
        with Session(engine) as session:
            query = select(TPRM.name, TPRM.val_type).where(
                TPRM.tmo_id == request.tmo_id
            )
            if len(request.columns) > 0:
                query = query.where(TPRM.name.in_(request.columns))
            response = session.execute(query)
            mapper = {res[0]: res[1] for res in response.fetchall()}
            return dataflow_manager_pb2.ResponseGetTPRMNameToTypeMapper(
                mapper=mapper
            )
