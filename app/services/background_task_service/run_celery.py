import json
import os
import pickle
from dataclasses import dataclass
from http import HTTPStatus

from celery import Celery

from config.background_task_config import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
)
from database import get_not_auth_session
from routers.batch_router.exceptions import BatchCustomException
from routers.batch_router.processors import (
    BatchImportCreator,
    BatchImportPreview,
    BatchExportProcessor,
)
from routers.history_router.processors import ExportHistoryToEventManager
from services.kafka_service.producer.protobuf_producer import SendMessageToKafka

background_manager = Celery(
    main="background_manager",
    backend=CELERY_BROKER_URL,
    broker=CELERY_RESULT_BACKEND,
)

current_file_name = os.path.splitext(os.path.basename(__file__))[0]


@dataclass
class BackgroundResponse:
    status_code: int
    response_message: str


@background_manager.task(
    name=f"{current_file_name}.background_batch_import_creator"
)
def background_batch_import_creator(
    file: bytes,
    object_type_id: int,
    column_name_mapping: dict[str, str],
    delimiter: str,
    file_content_type: str,
    check: bool,
    pickled_user_data: str,
    force: bool,
):
    try:
        for session in get_not_auth_session():
            session.info.update(pickle.loads(bytes.fromhex(pickled_user_data)))
            task = BatchImportCreator(
                file=file,
                session=session,
                object_type_id=object_type_id,
                column_name_mapping=column_name_mapping,
                delimiter=delimiter,
                check=check,
                file_content_type=file_content_type,
                force=force,
            )

            response = task.execute()

            return BackgroundResponse(
                status_code=HTTPStatus.OK.value,
                response_message=json.dumps(response),
            ).__dict__

    except BatchCustomException as e:
        return BackgroundResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
            response_message=str(e.detail),
        ).__dict__


@background_manager.task(
    name=f"{current_file_name}.background_batch_import_preview"
)
def background_batch_import_preview(
    file: bytes,
    object_type_id: int,
    column_name_mapping: dict[str, str],
    delimiter: str,
    file_content_type: str,
    pickled_user_data: str,
):
    for session in get_not_auth_session():
        session.info.update(pickle.loads(bytes.fromhex(pickled_user_data)))
        try:
            task = BatchImportPreview(
                file=file,
                session=session,
                object_type_id=object_type_id,
                column_name_mapping=column_name_mapping,
                delimiter=delimiter,
                file_content_type=file_content_type,
            )
            output = task.execute()

            return BackgroundResponse(
                status_code=HTTPStatus.OK.value,
                response_message=str(output.read()),
            ).__dict__

        except BatchCustomException as e:
            return BackgroundResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                response_message=str(e.detail),
            ).__dict__


@background_manager.task(
    name=f"{current_file_name}.background_batch_export_task"
)
def background_batch_export_task(
    object_type_id: int,
    pickled_request: str,
    file_type: str,
    delimiter: str,
    pickled_obj_ids: str,
    pickled_prm_type_ids: str,
    replace_ids_by_names: bool,
    pickled_user_data: str,
):
    try:
        for session in get_not_auth_session():
            session.info.update(pickle.loads(bytes.fromhex(pickled_user_data)))
            task = BatchExportProcessor(
                session=session,
                object_type_id=object_type_id,
                delimiter=delimiter,
                request=pickle.loads(bytes.fromhex(pickled_request)),
                file_type=file_type,
                obj_ids=pickle.loads(bytes.fromhex(pickled_obj_ids)),
                prm_type_ids=pickle.loads(bytes.fromhex(pickled_prm_type_ids)),
                replace_ids_by_names=replace_ids_by_names,
            )
            task.check()
            output = task.execute()

            return BackgroundResponse(
                status_code=HTTPStatus.OK.value,
                response_message=str(output.read()),
            ).__dict__

    except BatchCustomException as e:
        return BackgroundResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
            response_message=str(e.detail),
        ).__dict__


@background_manager.task(
    name=f"{current_file_name}.background_manager_send_kafka_message"
)
def background_manager_send_kafka_message(
    key_class_name: str, key_event: str, data_to_send: str
):
    """
    "data_to_send" have to be pickled
    """
    task = SendMessageToKafka(
        key_class_name=key_class_name,
        key_event=key_event,
        data_to_send=pickle.loads(bytes.fromhex(data_to_send)),
    )
    task.send_message()


@background_manager.task(name=f"{current_file_name}.background_events_history")
def background_events_history(token: str, host: str):
    for session in get_not_auth_session():
        task = ExportHistoryToEventManager(
            session=session, token=token, host=host
        )
        task.process()
        return 1
