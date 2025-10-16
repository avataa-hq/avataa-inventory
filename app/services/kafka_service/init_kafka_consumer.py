import asyncio

from resistant_kafka_avataa import ConsumerConfig
from resistant_kafka_avataa.common_schemas import KafkaSecurityConfig
from resistant_kafka_avataa.consumer import process_kafka_connection
from resistant_kafka_avataa.message_desirializers import MessageDeserializer

from config.kafka_config import (
    KAFKA_URL,
    KAFKA_CONSUMER_GROUP_ID,
    KAFKA_CONSUMER_OFFSET,
    KAFKA_SECURED,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISMS,
    KAFKA_DOCUMENTS_CHANGES_TOPIC,
    KAFKA_TURN_ON,
)
from services.grpc_service.proto_files.kafka_documents.files.kafka_document_pb2 import (
    Document,
)
from services.kafka_service.consumer.processors.documents_changes_processor import (
    DocumentsChangesProcessor,
)
from services.kafka_service.kafka_connection_utils import (
    get_token_for_kafka_by_keycloak,
)


def init_kafka_connection():
    if KAFKA_TURN_ON:
        config = ConsumerConfig(
            topic_to_subscribe=KAFKA_DOCUMENTS_CHANGES_TOPIC,
            processor_name="DocumentsChangesProcessor",
            bootstrap_servers=KAFKA_URL,
            group_id=KAFKA_CONSUMER_GROUP_ID,
            auto_offset_reset=KAFKA_CONSUMER_OFFSET,
            enable_auto_commit=False,
        )

        if KAFKA_SECURED:
            config.security_config = KafkaSecurityConfig(
                oauth_cb=get_token_for_kafka_by_keycloak,
                security_protocol=KAFKA_SECURITY_PROTOCOL,
                sasl_mechanisms=KAFKA_SASL_MECHANISMS,
            )

        deserializers = MessageDeserializer(
            topic=config.topic_to_subscribe,
        )
        deserializers.register_protobuf_deserializer(Document)

        inventory_changes_processor = DocumentsChangesProcessor(
            config=config, deserializers=deserializers
        )

        asyncio.create_task(
            process_kafka_connection([inventory_changes_processor])
        )
