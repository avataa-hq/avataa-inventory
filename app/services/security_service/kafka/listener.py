from collections import defaultdict
from enum import Enum
from sqlalchemy import event
from sqlalchemy.orm import Session

from services.security_service.data.permissions.permission_template import (
    PermissionTemplate,
)
from services.security_service import kafka
from services.security_service.kafka.kafka_producer import (
    SecurityEventStatus,
    security_kafka_producer,
)


class SessionDataKeys(Enum):
    NEW = "security_created_instances"
    DELETED = "security_deleted_instances"
    DIRTY = "security_updated_instances"


@event.listens_for(Session, "after_flush")
def after_flush(session: Session, flush_context):
    def session_data_handler(
        session_data, key_for_session_data: SessionDataKeys
    ):
        if not session.info.get(key_for_session_data.value, False):
            session.info.setdefault(
                key_for_session_data.value, defaultdict(list)
            )

        for item in session_data:
            if not isinstance(item, PermissionTemplate):
                continue
            item_class_name = type(item).__name__
            session.info[key_for_session_data.value][item_class_name].append(
                item
            )

    if session.new:
        session_data_handler(session.new, SessionDataKeys.NEW)

    if session.deleted:
        session_data_handler(session.deleted, SessionDataKeys.DELETED)

    if session.dirty:
        session_data_handler(session.dirty, SessionDataKeys.DIRTY)


@event.listens_for(Session, "after_commit")
def after_commit(session: Session):
    def after_commit_data_handler(
        key_for_session_data: SessionDataKeys,
        event: kafka.kafka_producer.SecurityEventStatus,
    ):
        data = session.info[key_for_session_data.value]
        del session.info[key_for_session_data.value]
        for class_name, items_data in data.items():
            security_kafka_producer.send(
                obj_class_name=class_name, event=event, items=items_data
            )

    if session.info.get(SessionDataKeys.NEW.value, False):
        after_commit_data_handler(
            SessionDataKeys.NEW, SecurityEventStatus.CREATED
        )

    if session.info.get(SessionDataKeys.DIRTY.value, False):
        after_commit_data_handler(
            SessionDataKeys.DIRTY, SecurityEventStatus.UPDATED
        )

    if session.info.get(SessionDataKeys.DELETED.value, False):
        after_commit_data_handler(
            SessionDataKeys.DELETED, SecurityEventStatus.DELETED
        )
