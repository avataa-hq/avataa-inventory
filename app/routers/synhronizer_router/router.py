import asyncio

import grpc
from fastapi import APIRouter, Depends, BackgroundTasks, Request
from google.protobuf import json_format
from grpc.aio import AioRpcError
from sqlmodel import select
from sqlmodel import Session

from config.grpc_config import DOCUMENTS_GRPC_HOST, DOCUMENTS_GRPC_PORT
from database import get_session
from models import MO
from services.grpc_service.proto_files.document_grpc.files import (
    documents_pb2_grpc,
    documents_pb2,
)
from services.background_task_service.run_celery import (
    background_events_history,
)

router = APIRouter(tags=["Synchronizer"])


@router.post("/synchronize/documents")
async def synchronize_documents(
    background_tasks: BackgroundTasks, session: Session = Depends(get_session)
):
    async def synchronize_object_document_count_value():
        async with grpc.aio.insecure_channel(
            f"{DOCUMENTS_GRPC_HOST}:{DOCUMENTS_GRPC_PORT}"
        ) as channel:
            try:
                stub = documents_pb2_grpc.DocumentInformerStub(channel)
                message_as_dict = {}
                msg = documents_pb2.RequestGetObjectDocumentCount(check=True)
                response_async_generator = stub.GetObjectDocumentCount(msg)
                async for item in response_async_generator:
                    grpc_data_response = json_format.MessageToDict(
                        item,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True,
                    )
                    message_as_dict = {
                        k: message_as_dict.get(k, 0) + v
                        for k, v in grpc_data_response[
                            "object_and_documents"
                        ].items()
                    }

                object_ids = list(message_as_dict.keys())
                exists_objects = session.exec(
                    select(MO).where(MO.id.in_(object_ids))
                ).all()
                for obj in exists_objects:
                    if obj.document_count != message_as_dict[str(obj.id)]:
                        obj.document_count = message_as_dict[str(obj.id)]

                session.commit()

                object_without_documents = session.exec(
                    select(MO).where(
                        MO.id.not_in(object_ids), MO.document_count != 0
                    )
                ).all()
                for obj in object_without_documents:
                    obj.document_count = 0

                session.commit()

            except AioRpcError as e:
                print(f"ERROR {e}")

    def run_synchronize_object_document_count_value():
        asyncio.run(synchronize_object_document_count_value())

    background_tasks.add_task(run_synchronize_object_document_count_value)
    return {"details": "Process started"}


@router.get("/export_history_to_event_manager")
async def export_history_to_event_manager(
    request: Request,
    session: Session = Depends(get_session),
):
    token = request.headers.get("Authorization")
    host = request.headers.get("host")

    background_events_history.delay(token, host)
    return {"task_id": "task_done"}
