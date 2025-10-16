from enum import Enum
from typing import Iterator

from confluent_kafka import Producer

from config import kafka_config
from services.grpc_service.proto_files.transfer.files.transfer_pb2 import (
    ListPermission,
    Permission,
)
from services.kafka_service.producer.utils import producer_config


class SecurityEventStatus(Enum):
    CREATED = "security.created"
    UPDATED = "security.updated"
    DELETED = "security.deleted"


class SecurityKafkaProducer:
    def __init__(self, topic: str):
        self.topic = topic
        config = producer_config()
        self.kafka_producer = Producer(config)
        self._chunk_size = 500

    def send(
        self, items: list | set, obj_class_name: str, event: SecurityEventStatus
    ):
        for chunked_items in self.get_chunks(data=items):
            prepared_data = self._prepare(
                obj_class_name=obj_class_name, event=event, items=chunked_items
            )
            self._send(
                msg=prepared_data, obj_class_name=obj_class_name, event=event
            )

    def _prepare(
        self, obj_class_name: str, event: SecurityEventStatus, items: list | set
    ) -> ListPermission:
        prepared_data = []
        for item in items:
            prepared_item = Permission(**item.to_dict())
            prepared_data.append(prepared_item)
        prepared_data = ListPermission(permission=prepared_data)
        return prepared_data

    def get_chunks(self, data: list | set) -> Iterator[list]:
        last_chunk_size = self._chunk_size
        iterator = 0
        while last_chunk_size == self._chunk_size:
            start = iterator * self._chunk_size
            end = start + self._chunk_size
            chunk = data[start:end]
            last_chunk_size = len(chunk)
            if chunk:
                yield chunk

    def _send(self, msg, obj_class_name: str, event: SecurityEventStatus):
        key = f"{obj_class_name}:{event.value}"
        self.kafka_producer.produce(
            topic=self.topic,
            value=msg.SerializeToString(),
            key=key,
            on_delivery=self._delivery_report,
        )
        self.kafka_producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed for User record {msg.key}: {err}")
            # TODO logging
            return

        print(
            f"User record {msg.key()} successfully produced "
            f"to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


security_kafka_producer = SecurityKafkaProducer(
    kafka_config.KAFKA_SECURITY_TOPIC
)
