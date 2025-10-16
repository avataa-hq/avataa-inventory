import json
from typing import Iterator

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.any_pb2 import Any  # noqa
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session
from fastapi.requests import Request
from fastapi.datastructures import Headers
from fastapi.responses import Response

from routers.object_router.router import read_objects
from routers.object_type_router.router import (
    read_child_object_types,
    update_object_type,
    read_object_types,
)
from routers.object_type_router.schemas import (
    TMOUpdate as InvTMOUpdate,
    TMOResponse as InvTMOResponse,
)

from models import MO as InvMO, TPRM as InvTPRM
from routers.parameter_type_router.router import read_object_type_param_types
from services.grpc_service.proto_files.zeebe.files.zeebe_to_inventory_pb2 import (
    InReadObjectTypes,
    TMO,
    OutTMOArray,
    InReadObjects,
    OutMOArray,
    InReadObjectTypeParamTypes,
    OutTPRMArray,
    TPRM,
    InUpdateObjectType,
    InReadChildObjectTypes,
    InGetTMOIdsByMoIds,
    OutGetTMOIdsByMoIds,
)
from services.grpc_service.proto_files.zeebe.files.zeebe_to_inventory_pb2_grpc import (
    ZeebeInformerServicer,
)


class ZeebeInformer(ZeebeInformerServicer):
    def __init__(self, engine: Engine):
        super().__init__()
        self.session_builder = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            class_=Session,
        )

    def _get_session(self) -> Iterator[Session]:
        with self.session_builder() as session:
            yield session

    @staticmethod
    def _build_request(
        method: str = "GET",
        server: str = "127.0.0.1",
        path: str = "/",
        headers: dict = None,
        body: str = None,
    ) -> Request:
        if headers is None:
            headers = {}
        request_ = Request(
            {
                "type": "http",
                "path": path,
                "headers": Headers(headers).raw,
                "http_version": "1.1",
                "method": method,
                "scheme": "https",
                "client": ("127.0.0.1", 8000),
                "server": (server, 443),
                "query_string": path[path.index("?") + 1 :]
                if "?" in path
                else None,
            }
        )
        if body:

            async def request_body():
                return body

            request_.body = request_body
        return request_

    async def ReadObjectTypes(
        self, request: InReadObjectTypes, context: grpc.aio.ServicerContext
    ) -> OutTMOArray:
        ids = list(request.id)
        results = []
        for session in self._get_session():
            results: list[InvTMOResponse] = await read_object_types(
                object_types_ids=ids, session=session
            )
        converted_results: list[TMO] = [
            TMO(**json.loads(r.json())) for r in results
        ]
        return OutTMOArray(tmo=converted_results)

    async def ReadObjects(
        self, request: InReadObjects, context: grpc.aio.ServicerContext
    ) -> OutMOArray:
        result = []
        request_dict: dict = MessageToDict(
            request,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )
        request_mock = self._build_request(
            path=request.query if request.query else "/"
        )
        response_mock = Response()

        exclude_params = ("query",)
        default_params = {
            "object_type_id": None,
            "p_id": None,
            "name": None,
            "obj_id": None,
            "with_parameters": False,
            "active": True,
            "limit": 50,
            "offset": 0,
            "order_by_tprms_id": None,
            "order_by_asc": None,
            "identifiers_instead_of_values": False,
        }
        other_params = {}
        for k, v in request_dict.items():
            if k in exclude_params:
                continue
            if not v and k in default_params:
                v = default_params[k]
            elif isinstance(v, list) and len(v) == 0 and k in default_params:
                v = default_params[k]
            other_params[k] = v

        for session in self._get_session():
            result = await read_objects(
                session=session,
                request=request_mock,
                response=response_mock,
                **other_params,
            )
        converted_result = [json.dumps(r) for r in result]
        return OutMOArray(array=converted_result)

    async def ReadObjectTypeParamTypes(
        self,
        request: InReadObjectTypeParamTypes,
        context: grpc.aio.ServicerContext,
    ) -> OutTPRMArray:
        results = []
        function_params = MessageToDict(
            request,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )
        for session in self._get_session():
            results: list[InvTPRM] = await read_object_type_param_types(
                session=session, **function_params
            )
        converted_results: list[TPRM] = [
            TPRM(**json.loads(r.json())) for r in results
        ]
        return OutTPRMArray(array=converted_results)

    async def UpdateObjectType(
        self, request: InUpdateObjectType, context: grpc.aio.ServicerContext
    ) -> TMO:
        update_obj: dict = MessageToDict(
            request.object_type,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )
        update_obj = {k: v for k, v in update_obj.items() if v is not None}
        for reset_param in request.reset_parameters:
            update_obj[reset_param] = None
        function_object_type = InvTMOUpdate(**update_obj)
        for session in self._get_session():
            result: InvTMOResponse = await update_object_type(
                session=session,
                object_type_id=request.id,
                object_type=function_object_type,
            )
            return TMO(**json.loads(result.json()))

    async def ReadChildObjectTypes(
        self, request: InReadChildObjectTypes, context: grpc.aio.ServicerContext
    ) -> OutTMOArray:
        results = []
        for session in self._get_session():
            results: list[InvTMOResponse] = await read_child_object_types(
                parent_id=request.parent_id, session=session
            )
        converted_results: list[TMO] = [
            TMO(**json.loads(r.json())) for r in results
        ]
        return OutTMOArray(tmo=converted_results)

    async def GetTMOIdsByMoIds(
        self, request: InGetTMOIdsByMoIds, context: grpc.aio.ServicerContext
    ) -> OutGetTMOIdsByMoIds:
        results = {}
        for session in self._get_session():
            stmt = select(InvMO.tmo_id, InvMO.id).filter(
                InvMO.id.in_(request.mo_ids)
            )  # noqa
            data = session.execute(stmt).mappings().all()
            results: dict = {i["id"]: i["tmo_id"] for i in data}
        return OutGetTMOIdsByMoIds(mapper=results)
