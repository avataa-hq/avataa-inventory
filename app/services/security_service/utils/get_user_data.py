from sqlalchemy.orm import Session

from services.security_service.security_data_models import UserData


def get_username_from_session(session: Session, default: str = ""):
    """Returns username from session, otherwise returns default value"""
    user_data = session.info.get("jwt")

    if not user_data:
        return default

    user_name = getattr(user_data, "preferred_name", None)

    if not user_name:
        return default

    return user_name


def get_user_id_from_session(session: Session):
    user_data = session.info.get("jwt")

    if user_data:
        return user_data.id

    return None


def get_session_id_from_session(session: Session):
    user_data: UserData = session.info.get("jwt")

    if user_data:
        return user_data.session_id

    return None
