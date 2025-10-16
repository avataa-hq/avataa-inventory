import pickle
from typing import Union, List

from fastapi import (
    APIRouter,
    File,
    UploadFile,
    Depends,
    Query,
    Request,
    Form,
    HTTPException,
)
from sqlmodel import Session
from starlette.background import BackgroundTasks
from starlette.responses import StreamingResponse

from database import get_session
from functions.db_functions.db_read import get_db_object_type_or_exception
from models import BackgroundTask
from routers.batch_router.constants import (
    RESULT_PREVIEW_FILE_NAME,
    RESULT_EXPORT_FILE_NAME,
)
from routers.batch_router.exceptions import BatchCustomException
from routers.batch_router.processors import (
    BatchExportProcessor,
    BatchImportPreview,
    BatchImportCreator,
)
from routers.batch_router.schemas import ExportFileTypes
from routers.batch_router.utils import parse_column_name_mapping
from services.background_task_service.run_celery import (
    background_batch_import_preview,
    background_batch_import_creator,
    background_batch_export_task,
)
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)

router = APIRouter(prefix="/batch", tags=["Batch operations"])


@router.post(
    "/object_and_param_values/{tmo_id}",
    status_code=201,
    description="""
        This endpoint creates or objects and parameters.
        :param column_name_mapping: dictionary, that have goal to replace already exists TPRM names by aliased.
                                    Keys of dict: aliased names, and values are real TPRM names.

        file: file with type: csv/xlsx

        delimiter: delimiter for csv file

        check: if check is True: parameter and objects will not be created,but previous create/update/delete
        statistic will be return.
               if check is False: parameters and objects will be created

        force: if force is True: even if in file there are not correct values - endpoint will create only
        correct data and will ignore errors.
               if force is False: there are will be raised errors because of not correct data

        session: Session

        background_tasks: BackgroundTasks

        tmo_id: Object Type ID
    """,
)
def batch_objects_import(
    tmo_id: int,
    background_tasks: BackgroundTasks,
    column_name_mapping: dict = Depends(parse_column_name_mapping),
    file: UploadFile = File(),
    delimiter: str = Form(default=",", max_length=1, min_length=1),
    check: bool = Form(default=False),
    force: bool = Form(default=False),
    session: Session = Depends(get_session),
):
    try:
        task = BatchImportCreator(
            file=file.file.read(),
            session=session,
            object_type_id=tmo_id,
            column_name_mapping=column_name_mapping,
            delimiter=delimiter,
            check=check,
            file_content_type=file.content_type,
            background_tasks=background_tasks,
            force=force,
        )
        response = task.execute()
        return response

    except BatchCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/batch_objects_preview/{tmo_id}", status_code=200)
def batch_objects_preview(
    tmo_id: int,
    session: Session = Depends(get_session),
    column_name_mapping: dict = Depends(parse_column_name_mapping),
    file: UploadFile = File(),
    delimiter: str = Form(default=",", max_length=1, min_length=1),
):
    try:
        task = BatchImportPreview(
            file=file.file.read(),
            session=session,
            object_type_id=tmo_id,
            column_name_mapping=column_name_mapping,
            delimiter=delimiter,
            file_content_type=file.content_type,
        )
        output = task.execute()

        return StreamingResponse(
            output,
            headers={
                "Content-Disposition": f'attachment; filename="{RESULT_PREVIEW_FILE_NAME}"'
            },
        )

    except BatchCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post(
    "/background_object_and_param_values/{tmo_id}",
    status_code=201,
    description="Allows to import objects and their values from csv file by the file."
    "In csv file the headers for the Object(MO) fields must be equal to the name of MO"
    " attribute (example: geometry, pov, description). The headers for Parameter "
    "Values of Object must be equal to their ids in database (TPRM.id)",
)
def background_object_and_param_values(
    tmo_id: int,
    column_name_mapping: dict = Depends(parse_column_name_mapping),
    file: UploadFile = File(),
    delimiter: str = Form(default=",", max_length=1, min_length=1),
    check: bool = Form(default=False),
    force: bool = Form(default=False),
    session: Session = Depends(get_session),
):
    get_db_object_type_or_exception(session=session, object_type_id=tmo_id)
    task_id = background_batch_import_creator.delay(
        file.file.read(),
        tmo_id,
        column_name_mapping,
        delimiter,
        file.content_type,
        check,
        pickle.dumps(session.info).hex(),
        force,
    )
    background_task = BackgroundTask(
        task_id=str(task_id),
        task_name="batch_import",
        username=get_username_from_session(session=session),
        object_type_id=tmo_id,
    )
    session.add(background_task)
    session.commit()

    return {"task_id": str(task_id)}


@router.post("/background_batch_objects_preview/{tmo_id}", status_code=201)
def background_batch_objects_preview(
    tmo_id: int,
    column_name_mapping: dict = Depends(parse_column_name_mapping),
    file: UploadFile = File(),
    delimiter: str = Form(default=",", max_length=1, min_length=1),
    session: Session = Depends(get_session),
):
    get_db_object_type_or_exception(session=session, object_type_id=tmo_id)

    task_id = background_batch_import_preview.delay(
        file.file.read(),
        tmo_id,
        column_name_mapping,
        delimiter,
        file.content_type,
        pickle.dumps(session.info).hex(),
    )
    background_task = BackgroundTask(
        task_id=str(task_id),
        task_name="batch_preview",
        username=get_username_from_session(session=session),
        object_type_id=tmo_id,
    )
    session.add(background_task)
    session.commit()

    return {"task_id": str(task_id)}


@router.post(
    "/background_batch_objects_export/{object_type_id}", status_code=201
)
def background_batch_export(
    object_type_id: int,
    request: Request,
    file_type: ExportFileTypes,
    session: Session = Depends(get_session),
    delimiter: str = Query(default=";", max_length=1, min_length=1),
    obj_ids: Union[List[int], None] = Query(default=None),
    prm_type_ids: Union[List[int], None] = Query(default=None),
    replace_ids_by_names: bool = Query(default=False),
):
    get_db_object_type_or_exception(
        session=session, object_type_id=object_type_id
    )

    def pickle_request(request_data: Request):
        request_data = {
            "method": request_data.method,
            "url": str(request_data.url),
            "headers": dict(request_data.headers),
            "query_params": dict(request_data.query_params),
            "path_params": request_data.path_params,
            "client": request_data.client.host if request_data.client else None,
        }

        return pickle.dumps(request_data).hex()

    if not prm_type_ids:
        prm_type_ids = []

    if not obj_ids:
        obj_ids = []

    task_id = background_batch_export_task.delay(
        object_type_id,
        pickle_request(request_data=request),
        file_type.value,
        delimiter,
        pickle.dumps(obj_ids).hex(),
        pickle.dumps(prm_type_ids).hex(),
        replace_ids_by_names,
        pickle.dumps(session.info).hex(),
    )

    background_task = BackgroundTask(
        task_id=str(task_id),
        task_name="batch_export",
        username=get_username_from_session(session=session),
        object_type_id=object_type_id,
    )
    session.add(background_task)
    session.commit()

    return {"task_id": str(task_id)}


@router.get(
    "/export_obj_with_params/{object_type_id}",
    status_code=200,
    description="Allows to export objects (with particular TMO id) with their parameters values into "
    "csv or xlsx.",
)
def batch_objects_export(
    object_type_id: int,
    request: Request,
    file_type: ExportFileTypes,
    session: Session = Depends(get_session),
    delimiter: str = Query(default=";", max_length=1, min_length=1),
    obj_ids: Union[List[int], None] = Query(default=None),
    prm_type_ids: Union[List[int], None] = Query(default=None),
    replace_ids_by_names: bool = Query(default=False),
    with_full_attributes: bool = Query(default=False),
):
    try:
        request_data = {
            "method": request.method,
            "url": str(request.url),
            "headers": dict(request.headers),
            "query_params": dict(request.query_params),
            "path_params": request.path_params,
            "client": request.client.host if request.client else None,
        }

        task = BatchExportProcessor(
            session=session,
            object_type_id=object_type_id,
            delimiter=delimiter,
            request=request_data,
            file_type=file_type.value,
            obj_ids=obj_ids,
            prm_type_ids=prm_type_ids,
            replace_ids_by_names=replace_ids_by_names,
            with_full_attributes=with_full_attributes,
        )
        task.check()
        output = task.execute()

        return StreamingResponse(
            output,
            headers={
                "Content-Disposition": f'attachment; filename="{RESULT_EXPORT_FILE_NAME}.{file_type.value}"'
            },
        )

    except BatchCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
