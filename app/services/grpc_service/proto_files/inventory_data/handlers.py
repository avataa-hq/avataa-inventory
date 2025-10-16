"""Handler for request from gRPC server"""

import pickle

from logging import getLogger
from typing import AsyncGenerator, Any

from sqlmodel import Session

from database import engine
from functions.db_functions.db_read import get_objects_with_parameters
from routers.object_router.utils import TPRMFilterCleaner
from services.grpc_service.proto_files.inventory_data.files import (
    inventory_data_pb2,
)
from services.security_service.security_data_models import UserData


class FilteredObjWithParamsHandler(object):
    """Handler for Filter Object with parameters gRPC request. Return single object and divided by chunks"""

    def __init__(self, max_chunk_size: int = 1_000_000) -> None:
        self.logger = getLogger("Filtered Obj With Params")
        self.max_chunk_size = max_chunk_size
        self.engine = engine

    async def get_stream_response_chunked(
        self, request: inventory_data_pb2.RequestForFilteredObjInfoByTMO
    ) -> AsyncGenerator[inventory_data_pb2.ResponseMOdata, None]:
        objects = await self.process(request)
        current_chunk = []

        for obj in objects:
            current_chunk.append(obj)
            proto_msg = inventory_data_pb2.ResponseMOdata(
                objects_with_parameters=current_chunk
            )
            if proto_msg.ByteSize() > self.max_chunk_size:
                # remove last element
                current_chunk.pop()
                if current_chunk:
                    yield inventory_data_pb2.ResponseMOdata(
                        objects_with_parameters=current_chunk
                    )
                else:
                    # if obj more than max_chunk size
                    yield inventory_data_pb2.ResponseMOdata(
                        objects_with_parameters=[obj]
                    )
                    current_chunk = []
                    continue
                current_chunk = [obj]
        if current_chunk:
            yield inventory_data_pb2.ResponseMOdata(
                objects_with_parameters=current_chunk
            )

    async def process(
        self, request: inventory_data_pb2.RequestForFilteredObjInfoByTMO
    ) -> list[str]:
        try:
            elements: list[dict[str, str]] = await self._fetch_elements(request)
            validated: list[str] = self._prepare_results(elements)
            return validated
        except Exception as ex:
            self.logger.error(f"Processing failed: {str(ex)}")
            raise

    async def _fetch_elements(
        self, request: inventory_data_pb2.RequestForFilteredObjInfoByTMO
    ) -> list[dict[str, str]]:
        tprm_cleaner_data = dict()
        if request.object_type_id:
            tprm_cleaner_data["object_type_id"] = request.object_type_id

        if request.query_params:
            tprm_cleaner_data["query_params"] = pickle.loads(
                bytes.fromhex(request.query_params)
            )

        order_by = None
        if request.order_by:
            order_by = pickle.loads(bytes.fromhex(request.order_by))
        additional_filter_data = {}
        if request.mo_ids:
            additional_filter_data["obj_ids"] = request.mo_ids

        with Session(self.engine) as session:
            if request.decoded_jwt:
                session.info["jwt"] = UserData.from_jwt(
                    pickle.loads(bytes.fromhex(request.decoded_jwt))
                )
                session.info["action"] = "read"

            tprm_cleaner = TPRMFilterCleaner(
                session=session, **tprm_cleaner_data
            )
            if any(
                [
                    tprm_cleaner.check_filter_data_in_query_params(),
                    order_by,
                    tprm_cleaner_data,
                ]
            ):
                mos_ids = (
                    tprm_cleaner.get_mo_ids_which_match_clean_filter_conditions(
                        order_by=order_by, active=True, **additional_filter_data
                    )
                )
                objects_to_read = get_objects_with_parameters(
                    session,
                    limit=None,
                    offset=None,
                    mos_ids=mos_ids,
                    returnable=True,
                    active=True,
                )

        return objects_to_read

    def _prepare_results(self, data: list[dict[str, str]]) -> list[str]:
        result = []
        for item in data:
            result.append(pickle.dumps(self._modify_dict(item)).hex())
        return result

    @staticmethod
    def _modify_dict(item: dict[str, Any]) -> dict[str, str]:
        item["params"] = [dict(param) for param in item["params"]]
        return item
