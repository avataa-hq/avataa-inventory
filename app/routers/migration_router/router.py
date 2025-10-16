from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    UploadFile,
    File,
)
from sqlmodel import Session
from starlette.responses import StreamingResponse

from database import get_session
from routers.migration_router.constants import MIGRATION_EXPORT_FILENAME
from routers.migration_router.exceptions import MigrationException
from routers.migration_router.processors import (
    MigrateObjectTypeAsExport,
    MigrateObjectTypeAsImport,
)
from routers.migration_router.schemas import MigrateObjectTypeAsExportRequest

router = APIRouter(prefix="/migration", tags=["Migration"])


@router.post(path="/migrate_object_type_as_export", status_code=200)
def migrate_object_type(
    object_type_id: int,
    parents: bool,
    children: bool,
    session: Session = Depends(get_session),
):
    try:
        task = MigrateObjectTypeAsExport(
            session=session,
            request=MigrateObjectTypeAsExportRequest(
                object_type_id=object_type_id,
                parents=parents,
                children=children,
            ),
        )
        output = task.execute()

        return StreamingResponse(
            output,
            headers={
                "Content-Disposition": f'attachment; filename="{MIGRATION_EXPORT_FILENAME}.xlsx"'
            },
        )

    except MigrationException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post(path="/migrate_object_type_as_import")
def migrate_object_type_as_import(
    file: UploadFile = File(),
    session: Session = Depends(get_session),
):
    try:
        task = MigrateObjectTypeAsImport(session=session, file=file)
        task.execute()
        return {"status": "Objects migrated successfully"}

    except MigrationException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
