from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException, Depends
from sqlmodel import Session, select

from database import get_session
from models import Event
from routers.history_router.processors import ExportHistoryToEventsManager
from routers.object_type_router.constants import event_types, models_events

router = APIRouter(tags=["History"])


@router.get("/history")
async def get_history(
    model: List[str] = Query([]),
    event_type: List[str] = Query([]),
    user: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: Optional[int] = Query(default=50, gt=-1),
    offset: Optional[int] = Query(default=0, gt=-1),
    session: Session = Depends(get_session),
):
    for et in event_type:
        if et not in event_types:
            raise HTTPException(
                status_code=422, detail=f"Invalid event_type: {et}"
            )
    for m in model:
        if m not in models_events:
            raise HTTPException(status_code=422, detail=f"Invalid model: {m}")
        else:
            event_type.extend(models_events[m])

    event_type = set(event_type)

    history = (
        session.execute(
            select(Event)
            .where(
                Event.event_time >= date_from
                if date_from is not None
                else True,
                Event.event_time <= date_to if date_to is not None else True,
                Event.user == user if user is not None else True,
                Event.event_type.in_(event_type)
                if len(event_type) > 0
                else True,
            )
            .offset(offset)
            .limit(limit)
            .order_by(Event.event_time)
        )
        .scalars()
        .all()
    )
    return history


@router.post("/export_postgres_to_elastic")
async def export_postgres_to_elastic(session: Session = Depends(get_session)):
    task = ExportHistoryToEventsManager(session=session)
    await task.execute()
