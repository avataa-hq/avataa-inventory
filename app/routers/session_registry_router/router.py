from fastapi import (
    APIRouter,
    Depends,
)
from sqlmodel import Session

from database import get_session
from routers.session_registry_router.processors import (
    GetSessionRegistryByUserId,
)
from routers.session_registry_router.schemas import (
    SessionRegistryRequest,
    GetSessionRegistryByUserIdRequest,
)
from services.security_service.utils.get_user_data import (
    get_user_id_from_session,
)

router = APIRouter(tags=["Session Registry"])


@router.post("/session_registry/")
async def get_session_ids_by_user_id(
    request: SessionRegistryRequest,
    session: Session = Depends(get_session),
):
    task = GetSessionRegistryByUserId(
        session=session,
        request=GetSessionRegistryByUserIdRequest(
            user_id=get_user_id_from_session(session=session),
            **request.dict(exclude_unset=True),
        ),
    )
    return task.execute()
