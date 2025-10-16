from sqlalchemy.event import listen
from sqlalchemy.orm import Session

from services.listener_service.processor import ListenerService


def init_listener():
    listen(
        target=Session,
        identifier="after_flush",
        fn=ListenerService.receive_after_flush,
    )

    listen(
        target=Session,
        identifier="after_commit",
        fn=ListenerService.receive_after_commit,
    )
