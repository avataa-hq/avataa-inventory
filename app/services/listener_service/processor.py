from resistant_kafka_avataa import ProducerInitializer, ProducerConfig
from resistant_kafka_avataa.common_schemas import KafkaSecurityConfig
from sqlmodel import Session

from common.common_constant import MODEL_EQ_MESSAGE, ObjEventStatus
from config.kafka_config import (
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISMS,
    KAFKA_PRODUCER_TOPIC,
    KAFKA_SECURED,
    KAFKA_URL,
    KAFKA_PRODUCER_PART_TOPIC_NAME,
    KAFKA_TURN_ON,
)
from services.event_service.processor import (
    EventProcessor,
)
from services.kafka_service.kafka_connection_utils import (
    get_token_for_kafka_by_keycloak,
)
from services.kafka_service.producer.protobuf_producer import SendMessageToKafka
from services.listener_service.constants import SessionDataKeys, AdditionalData
from services.security_service.utils.get_user_data import (
    get_user_id_from_session,
    get_session_id_from_session,
)
from services.session_registry_service.processor import SessionRegistryService

security_config = None

if KAFKA_SECURED:
    security_config = KafkaSecurityConfig(
        oauth_cb=get_token_for_kafka_by_keycloak,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanisms=KAFKA_SASL_MECHANISMS,
    )

if KAFKA_TURN_ON:
    producer_task = ProducerInitializer(
        config=ProducerConfig(
            producer_name=KAFKA_PRODUCER_TOPIC,
            bootstrap_servers=KAFKA_URL,
            security_config=security_config,
        )
    )

    producer_task_with_partitions = ProducerInitializer(
        config=ProducerConfig(
            producer_name=KAFKA_PRODUCER_PART_TOPIC_NAME,
            bootstrap_servers=KAFKA_URL,
            security_config=security_config,
        )
    )


class ListenerService:
    @staticmethod
    def receive_after_flush(session: Session, flush_context):
        def session_data_handler(
            session_data, key_for_session_data: SessionDataKeys
        ):
            if not session.info.get(key_for_session_data.value, False):
                session.info.setdefault(key_for_session_data.value, dict())

            for item in session_data:
                item_class_name = type(item).__name__
                if item_class_name in MODEL_EQ_MESSAGE.keys():
                    if not session.info[key_for_session_data.value].get(
                        item_class_name, False
                    ):
                        session.info[key_for_session_data.value][
                            item_class_name
                        ] = list()
                    session.info[key_for_session_data.value][
                        item_class_name
                    ].append(item.to_proto())

        if session.new:
            session_data_handler(session.new, SessionDataKeys.NEW)

        if session.deleted:
            session_data_handler(session.deleted, SessionDataKeys.DELETED)

        if session.dirty:
            session_data_handler(session.dirty, SessionDataKeys.DIRTY)

    @staticmethod
    def receive_after_commit(session: Session):
        def after_commit_data_handler(
            key_for_session_data: SessionDataKeys,
            key_event: ObjEventStatus,
        ):
            data = session.info[key_for_session_data.value]
            del session.info[key_for_session_data.value]
            for instance_type, data_to_send in data.items():
                task = SessionRegistryService(session=session)
                task.process_user_session()

                if KAFKA_TURN_ON:
                    task = SendMessageToKafka(
                        additional_data=AdditionalData(
                            user_id=get_user_id_from_session(session=session),
                            session_id=get_session_id_from_session(
                                session=session
                            ),
                        ),
                        key_class_name=instance_type,
                        key_event=key_event.value,
                        data_to_send=data_to_send,
                        producer_manager=producer_task,
                        producer_manager_with_partitions=producer_task_with_partitions,
                    )
                    task.send_message()

                task = EventProcessor(
                    session=session,
                    key_class_name=instance_type,
                    key_event=key_event.value,
                    data_to_send=data_to_send,
                )
                task.execute()

        if session.info.get(SessionDataKeys.NEW.value, False):
            after_commit_data_handler(
                key_for_session_data=SessionDataKeys.NEW,
                key_event=ObjEventStatus.CREATED,
            )

        if session.info.get(SessionDataKeys.DIRTY.value, False):
            after_commit_data_handler(
                key_for_session_data=SessionDataKeys.DIRTY,
                key_event=ObjEventStatus.UPDATED,
            )

        if session.info.get(SessionDataKeys.DELETED.value, False):
            after_commit_data_handler(
                key_for_session_data=SessionDataKeys.DELETED,
                key_event=ObjEventStatus.DELETED,
            )
