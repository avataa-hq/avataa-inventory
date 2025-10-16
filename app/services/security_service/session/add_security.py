from sqlalchemy import false
from sqlalchemy.orm import Session
from fastapi.requests import Request

from services.security_service.security_data_models import UserData

PREFIX = "/security"

ACTIONS = {
    "POST": "create",
    "GET": "read",
    "PATCH": "update",
    "PUT": "update",
    "DELETE": "delete",
}


def _get_action(request: Request):
    actions = ACTIONS.get(request.method, false())
    if request.url.path.startswith(PREFIX):
        actions = "admin"
    return actions


def add_security_data(session: Session, request: Request, user_data: UserData):
    session.info["jwt"] = user_data
    session.info["action"] = _get_action(request)
    return session
