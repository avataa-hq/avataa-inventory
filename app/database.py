from typing import Union, Generator

from fastapi import Depends
from fastapi.requests import Request
from sqlmodel import create_engine, Session

from config.database_config import DATABASE_URL, DB_SCHEMA
from services.security_service.security_data_models import UserData
from services.security_service.security_factory import security
from services.security_service.session.add_security import add_security_data

SQLALCHEMY_LIMIT = 32_766

engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=100,
    connect_args={"options": f"-csearch_path={DB_SCHEMA}"},
    pool_pre_ping=True,
)


def get_session(request: Request, user_data: UserData = Depends(security)):
    with Session(engine, expire_on_commit=False) as session:
        add_security_data(session=session, request=request, user_data=user_data)
        yield session


def get_not_auth_session():
    with Session(engine, expire_on_commit=False) as session:
        yield session


def get_chunked_values_by_sqlalchemy_limit(
    values: Union[list, set, dict.keys],
) -> Generator:
    if not values:
        return []

    values = list(values)
    return (
        values[index : index + SQLALCHEMY_LIMIT]
        for index in range(0, len(values), SQLALCHEMY_LIMIT)
    )
