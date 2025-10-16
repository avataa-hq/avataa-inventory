from typing import Iterator

from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.orm import Session

from database import engine
from services.grpc_service.proto_files.transfer.files import (
    transfer_pb2,
    transfer_pb2_grpc,
)

import grpc

from services.security_service.data.permissions.inventory import (
    TPRMPermission,
    MOPermission,
    TMOPermission,
)


class Transfer(transfer_pb2_grpc.TransferServicer):
    def __init__(self):
        self._engine = engine
        self._chunk_size = 500

    async def GetTMOPermission(
        self, request: transfer_pb2.Empty, context: grpc.aio.ServicerContext
    ) -> Iterator[transfer_pb2.ListPermission]:
        query = select(TMOPermission)
        with Session(self._engine) as session:
            for partition in (
                session.execute(query)
                .yield_per(self._chunk_size)
                .partitions(self._chunk_size)
            ):
                yield self._format_results(partition)

    async def GetMOPermission(
        self, request: transfer_pb2.Empty, context: grpc.aio.ServicerContext
    ) -> Iterator[transfer_pb2.ListPermission]:
        query = select(MOPermission)
        with Session(self._engine) as session:
            for partition in (
                session.execute(query)
                .yield_per(self._chunk_size)
                .partitions(self._chunk_size)
            ):
                yield self._format_results(partition)

    async def GetTPMPermission(
        self, request: transfer_pb2.Empty, context: grpc.aio.ServicerContext
    ) -> Iterator[transfer_pb2.ListPermission]:
        query = select(TPRMPermission)
        with Session(self._engine) as session:
            for partition in (
                session.execute(query)
                .yield_per(self._chunk_size)
                .partitions(self._chunk_size)
            ):
                yield self._format_results(partition)

    def _format_results(
        self, permissions: list[Row]
    ) -> transfer_pb2.ListPermission:
        results = []
        for permission in permissions:
            permission_dict = permission[0].to_dict()
            result = transfer_pb2.Permission(**permission_dict)
            results.append(result)
        return transfer_pb2.ListPermission(permission=results)
