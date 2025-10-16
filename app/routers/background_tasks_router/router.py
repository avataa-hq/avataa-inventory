from dataclasses import dataclass
from datetime import datetime

from celery.result import AsyncResult
from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from database import get_session
from models import BackgroundTask, TMO
from services.background_task_service.run_celery import background_manager

router = APIRouter(prefix="/background_task", tags=["Background tasks"])


@dataclass
class TaskStatusResponse:
    task_status: str
    task_result: dict
    completed_date: datetime
    task_name: str


@router.get("/get_tasks_by_username/{username}")
def get_tasks_by_username(
    username: str, session: Session = Depends(get_session)
):
    stmt = select(BackgroundTask).where(BackgroundTask.username == username)
    tesk_info_by_username: list[BackgroundTask] = (
        session.execute(stmt).scalars().all()
    )

    response = []
    for task in tesk_info_by_username:
        task_result = AsyncResult(id=task.task_id, app=background_manager)

        task_result_info = TaskStatusResponse(
            task_status=task_result.state,
            task_result=task_result.result if task_result.ready() else None,
            completed_date=task_result.date_done,
            task_name=task.task_name,
        )
        task = dict(task)
        task.update(task_result_info.__dict__)
        task["object_type_name"] = session.get(TMO, task["object_type_id"]).name
        response.append(task)

    return response


@router.get("/get_tasks_by_id/{task_id}")
def get_tasks_by_id(task_id: str, session: Session = Depends(get_session)):
    stmt = select(BackgroundTask).where(BackgroundTask.task_id == task_id)
    tesk_info: BackgroundTask = session.execute(stmt).scalar()

    task_result = AsyncResult(id=tesk_info.task_id, app=background_manager)

    task_result_info = TaskStatusResponse(
        task_status=task_result.state,
        task_result=task_result.result if task_result.ready() else None,
        completed_date=task_result.date_done,
        task_name=tesk_info.task_name,
    )

    task = dict(tesk_info)
    task.update(task_result_info.__dict__)
    task["object_type_name"] = session.get(TMO, tesk_info.object_type_id).name
    return task
