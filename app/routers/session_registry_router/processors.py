from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlmodel import select

from models import SessionRegistry
from routers.session_registry_router.schemas import (
    GetSessionRegistryByUserIdResponse,
    GetSessionRegistryByUserIdRequest,
)


class GetSessionRegistryByUserId:
    def __init__(
        self,
        session: Session,
        request: GetSessionRegistryByUserIdRequest,
    ):
        self._request = request
        self._session = session

    def _get_where_filter_query(self) -> list:
        where_conditions = [SessionRegistry.user_id == self._request.user_id]

        if self._request.datetime_from:
            where_conditions.append(
                SessionRegistry.activation_datetime
                > self._request.datetime_from
            )

        if self._request.datetime_to:
            where_conditions.append(
                SessionRegistry.activation_datetime < self._request.datetime_to
            )

        return where_conditions

    def _get_total_quantity(self, filters: list) -> int:
        query = (
            select(func.count()).select_from(SessionRegistry).where(*filters)
        )
        return self._session.exec(query).first()

    def _get_response_data(self, filters: list):
        query = (
            select(SessionRegistry)
            .where(*filters)
            .limit(self._request.limit)
            .offset(self._request.offset)
        )
        return self._session.execute(query).scalars().all()

    def execute(self):
        if self._request.user_id:
            filters = self._get_where_filter_query()

            return GetSessionRegistryByUserIdResponse(
                response_data=self._get_response_data(filters=filters),
                total=self._get_total_quantity(filters=filters),
            )

        return GetSessionRegistryByUserIdResponse(
            response_data=[],
            total=0,
        )
