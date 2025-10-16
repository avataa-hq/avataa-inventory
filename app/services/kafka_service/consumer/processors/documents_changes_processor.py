from resistant_kafka_avataa import (
    ConsumerInitializer,
    ConsumerConfig,
    kafka_processor,
)
from resistant_kafka_avataa.message_desirializers import MessageDeserializer
from sqlalchemy import select

from common.common_constant import DocumentsEventType
from database import get_not_auth_session
from models import MO


class DocumentsChangesProcessor(ConsumerInitializer):
    def __init__(
        self, config: ConsumerConfig, deserializers: MessageDeserializer = None
    ):
        super().__init__(config=config, deserializers=deserializers)
        self._config = config
        self._deserializers = deserializers
        self._session = next(get_not_auth_session())

    def _adapter_increase_object_document_count(self, value):
        stmt = select(MO).where(MO.id.in_(value.mo_id))
        object_instances = self._session.execute(stmt)
        object_instances = object_instances.scalars().all()

        for object_instance in object_instances:
            object_instance.document_count = object_instance.document_count + 1
            self._session.add(object_instance)
            self._session.flush()

        self._session.commit()

    def _adapter_decrease_object_document_count(self, value):
        stmt = select(MO).where(MO.id.in_(value.mo_id))
        object_instances = self._session.execute(stmt)
        object_instances = object_instances.scalars().all()

        for object_instance in object_instances:
            if object_instance.document_count > 0:
                object_instance.document_count = (
                    object_instance.document_count - 1
                )

            else:
                object_instance.document_count = 0

            self._session.add(object_instance)

        self._session.commit()

    @kafka_processor(store_error_messages=False)
    async def process(self, message):
        message_key = message.key().decode("utf-8")
        message_value = self._deserializers.deserialize(message, key="Document")

        match message_key:
            case DocumentsEventType.CREATED.value:
                self._adapter_increase_object_document_count(
                    value=message_value
                )

            case DocumentsEventType.DELETED.value:
                self._adapter_decrease_object_document_count(
                    value=message_value
                )
