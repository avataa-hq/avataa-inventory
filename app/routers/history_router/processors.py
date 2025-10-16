import json
from datetime import datetime
from typing import Sequence

import grpc
import requests
from resistant_kafka_avataa import ProducerInitializer, ProducerConfig
from resistant_kafka_avataa.common_schemas import KafkaSecurityConfig
from sqlalchemy import select, asc, func
from sqlalchemy.orm import Session

from config.grpc_config import EVENT_MANAGER_GRPC_PORT, EVENT_MANAGER_GRPC_HOST
from config.kafka_config import (
    KAFKA_URL,
    KAFKA_EVENTS_PRODUCER_TOPIC,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISMS,
)
from config.security_config import (
    KEYCLOAK_REALM,
    KEYCLOAK_REDIRECT_HOST,
    KEYCLOAK_REDIRECT_PROTOCOL,
)
from models import Event
from services.grpc_service.proto_files.event_manager_methods.files import (
    event_manager_pb2_grpc,
    event_manager_pb2,
)
from services.kafka_service.kafka_connection_utils import (
    get_token_for_kafka_by_keycloak,
)


class ExportHistoryToEventManager:
    EVENT_TYPE_MAPPING = {
        "Create": "CREATED",
        "Update": "UPDATED",
        "Delete": "DELETED",
    }

    EVENT_TYPES_BY_INSTANCE = {
        "TMO": ["TMOCreate", "TMOUpdate", "TMODelete"],
        "TPRM": ["TPRMCreate", "TPRMUpdate", "TPRMDelete"],
        "MO": ["MOCreate", "MOUpdate", "MODelete", "MOArchived"],
        "PRM": ["PRMCreate", "PRMUpdate", "PRMDelete"],
    }

    def __init__(self, session: Session, token: str, host: str):
        self._session = session
        self._token = token
        self._host = host
        self._user_id_by_username = {}

    def _process_event_category(
        self, instance_type: str, event_types: list[str]
    ):
        start_datetime = datetime.utcnow()

        count_query = select(func.count()).where(
            Event.event_type.in_(event_types),
            Event.event_time < start_datetime,
        )
        total_count = self._session.execute(count_query).scalar()

        for offset in range(0, total_count, 1_000):
            query = (
                select(Event)
                .where(
                    Event.event_type.in_(event_types),
                    Event.event_time < start_datetime,
                )
                .order_by(asc(Event.model_id))
                .offset(offset)
                .limit(1_000)
            )
            object_event_instances_generator = self._session.execute(query)

            instances = []
            for object_event_instances in object_event_instances_generator:
                for object_event_instance in object_event_instances:
                    instance = object_event_instance.event[instance_type]

                    if instance_type == "PRM":
                        match object_event_instance.event_type:
                            case "PRMCreate":
                                instance["creation_date"] = str(
                                    object_event_instance.event_time
                                )
                                instance["modification_date"] = None

                            case "PRMUpdate":
                                instance["modification_date"] = str(
                                    object_event_instance.event_time
                                )

                    key_event = self.EVENT_TYPE_MAPPING.get(
                        object_event_instance.event_type.replace(
                            instance_type, ""
                        ),
                        "UPDATED",
                    )
                    instance["user_id"] = self._user_id_by_username.get(
                        object_event_instance.user, "None"
                    )
                    instance["event_type"] = key_event
                    instances.append(instance)

            self._send_request_to_event_manager(
                instance=instance_type, data=instances
            )

    def _get_user_ids_by_username(self):
        url = f"{KEYCLOAK_REDIRECT_PROTOCOL}://{KEYCLOAK_REDIRECT_HOST}/admin/realms/{KEYCLOAK_REALM}/users"
        response = requests.get(
            url=url, headers={"Authorization": self._token}, timeout=30
        )
        return {
            user_instance["username"]: user_instance["id"]
            for user_instance in response.json()
        }

    def _send_request_to_event_manager(self, instance: str, data: list[dict]):
        url = f"https://{self._host}/api/event_manager/v1/events/create_events"
        requests.post(
            url=url, json={"instance": instance, "data": data}, timeout=150
        )

    def process(self):
        self._user_id_by_username = self._get_user_ids_by_username()
        for instance_type, event_types in self.EVENT_TYPES_BY_INSTANCE.items():
            self._process_event_category(
                instance_type=instance_type, event_types=event_types
            )


class ExportHistoryToEventsManager:
    def __init__(self, session: Session):
        self._session = session
        self.EVENT_TYPE_MAPPING = {
            "Create": "CREATED",
            "Update": "UPDATED",
            "Delete": "DELETED",
        }

        self.EVENT_TYPES_BY_INSTANCE = {
            # "TMO": ["TMOCreate", "TMOUpdate", "TMODelete"],
            # "TPRM": ["TPRMCreate", "TPRMUpdate", "TPRMDelete"],
            # "MO": ["MOCreate", "MOUpdate", "MOArchived", "MODelete"],
            "PRM": ["PRMCreate", "PRMUpdate", "PRMDelete"],
        }

        security_config = KafkaSecurityConfig(
            oauth_cb=get_token_for_kafka_by_keycloak,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            sasl_mechanisms=KAFKA_SASL_MECHANISMS,
        )

        self._producer_task = ProducerInitializer(
            config=ProducerConfig(
                producer_name=KAFKA_EVENTS_PRODUCER_TOPIC,
                bootstrap_servers=KAFKA_URL,
                security_config=security_config,
            )
        )

        def pass_delivery_report(*args):
            pass

        self._producer_task._delivery_report = pass_delivery_report

    def _get_event_key_by_event_type(self, event_type: str, instance_name: str):
        return self.EVENT_TYPE_MAPPING.get(
            event_type.replace(instance_name, ""),
            "UPDATED",
        )

    async def execute(self):
        channel_options = [
            ("grpc.keepalive_time_ms", 10_000),
            ("grpc.keepalive_timeout_ms", 15_000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ]

        async with grpc.aio.insecure_channel(
            target=f"{EVENT_MANAGER_GRPC_HOST}:{EVENT_MANAGER_GRPC_PORT}",
            options=channel_options,
        ) as channel:
            stub = event_manager_pb2_grpc.EventManagerInformerStub(channel)

            async def generate_events():
                for (
                    instance_name,
                    event_types,
                ) in self.EVENT_TYPES_BY_INSTANCE.items():
                    for event_type in event_types:
                        limit = 10_000
                        last_id = 0

                        if event_type == "PRMCreate":
                            last_id = 30_628_188

                        while True:
                            query = (
                                select(Event)
                                .where(
                                    Event.event_type == event_type,
                                    Event.model_id.is_not(None),
                                    Event.model_id > last_id,
                                )
                                .order_by(asc(Event.model_id))
                                .limit(limit)
                            )
                            instances: Sequence[Event] = (
                                self._session.execute(query).scalars().all()
                            )

                            if not instances:
                                break

                            for instance in instances:
                                if (
                                    instance.event_type
                                    in self.EVENT_TYPES_BY_INSTANCE["PRM"]
                                ):
                                    if instance.event_type == "PRMCreate":
                                        instance.event[instance_name][
                                            "creation_date"
                                        ] = str(instance.event_time)
                                    else:
                                        instance.event[instance_name][
                                            "modification_date"
                                        ] = str(instance.event_time)

                                last_id = instance.model_id
                                event_data = instance.__dict__["event"][
                                    instance_name
                                ]
                                yield event_manager_pb2.NewEventRequest(
                                    type=self._get_event_key_by_event_type(
                                        event_type=event_type,
                                        instance_name=instance_name,
                                    ),
                                    instance_name=instance_name,
                                    data=json.dumps(event_data),
                                )

            async for response in stub.NewEvent(generate_events()):
                pass
