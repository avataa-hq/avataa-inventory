import asyncio
import logging

import grpc

from services.grpc_service.proto_files.transfer.files import (
    transfer_pb2_grpc,
    transfer_pb2,
)


async def run_get_tmo() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = transfer_pb2_grpc.TransferStub(channel)
        msg = transfer_pb2.Empty()
        response_async_generator = stub.GetTMOPermission(msg)

        async for item in response_async_generator:
            print(item)


if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(run_get_tmo())
