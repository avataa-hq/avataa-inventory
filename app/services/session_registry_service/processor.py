import datetime

from sqlalchemy.orm import Session
from sqlmodel import select

from database import get_not_auth_session
from models import SessionRegistry, SessionRegistryStatus
from services.security_service.utils.get_user_data import (
    get_user_id_from_session,
    get_session_id_from_session,
)


class SessionRegistryService:
    def __init__(self, session: Session):
        self._session = session

    @staticmethod
    def _user_session_is_already_exists(
        new_session: Session, user_id: str, session_id: str
    ) -> SessionRegistry | None:
        query = select(SessionRegistry).where(
            SessionRegistry.user_id == user_id,
            SessionRegistry.session_id == session_id,
        )
        session_registry_instance = new_session.execute(query).scalar()
        return session_registry_instance

    @staticmethod
    def _add_new_session_registry(
        new_session: Session, user_id: str, session_id: str
    ):
        session_instance = SessionRegistry(
            user_id=user_id,
            session_id=session_id,
        )
        new_session.add(session_instance)
        new_session.flush()

    @staticmethod
    def _update_previous_session_registry(user_id: str, new_session: Session):
        query = select(SessionRegistry).where(
            SessionRegistry.user_id == user_id,
            SessionRegistry.status == SessionRegistryStatus.ACTIVE.value,
        )
        session_registry_instance = new_session.execute(query).scalar()
        session_registry_instance.status = SessionRegistryStatus.INACTIVE.value
        session_registry_instance.deactivation_datetime = (
            datetime.datetime.utcnow()
        )

        new_session.add(session_registry_instance)
        new_session.flush()

    def process_user_session(self):
        user_id = get_user_id_from_session(session=self._session)

        if user_id:
            new_session = next(get_not_auth_session())
            new_session.commit()

            user_id = get_user_id_from_session(session=self._session)
            session_id = get_session_id_from_session(session=self._session)

            current_user_session_instance = (
                self._user_session_is_already_exists(
                    new_session=new_session,
                    user_id=user_id,
                    session_id=session_id,
                )
            )
            if current_user_session_instance:
                return

            self._add_new_session_registry(
                user_id=user_id, session_id=session_id, new_session=new_session
            )

            self._update_previous_session_registry(
                user_id=user_id,
                new_session=new_session,
            )
            new_session.commit()
