import grpc
from google.protobuf.struct_pb2 import Struct
from sqlalchemy import asc
from sqlalchemy.orm import Session
from sqlmodel import select

from database import engine
from models import Event
from services.grpc_service.proto_files.event_manager_methods.files import (
    event_manager_pb2,
)
from services.grpc_service.proto_files.event_manager_methods.files.event_manager_pb2 import (
    NewEventRequest,
)
from services.grpc_service.proto_files.event_manager_methods.files.event_manager_pb2_grpc import (
    EventManagerInformerServicer,
)

EVENT_TYPE_MAPPING = {
    "Create": "CREATED",
    "Update": "UPDATED",
    "Delete": "DELETED",
}

EVENT_TYPES_BY_INSTANCE = {
    "TMO": ["TMOCreate", "TMOUpdate", "TMODelete"],
    "TPRM": ["TPRMCreate", "TPRMUpdate", "TPRMDelete"],
    "MO": ["MOCreate", "MOUpdate", "MOArchived", "MODelete"],
    "PRM": ["PRMCreate", "PRMUpdate", "PRMDelete"],
}


def _get_event_key_by_event_type(event_type: str, instance_name: str):
    return EVENT_TYPE_MAPPING.get(
        event_type.replace(instance_name, ""),
        "UPDATED",
    )


class EventManagerManager(EventManagerInformerServicer):
    def NewEvent(
        self,
        request: NewEventRequest,
        context: grpc.ServicerContext,
    ):
        with Session(engine) as session:
            for instance_name, event_types in EVENT_TYPES_BY_INSTANCE.items():
                for event_type in event_types:
                    limit = 1000
                    last_id = 0

                    event_key = _get_event_key_by_event_type(
                        event_type=event_type,
                        instance_name=instance_name,
                    )

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

                        instances = session.execute(query).scalars().all()
                        if not instances:
                            break

                        for instance in instances:
                            last_id = instance.model_id

                            event_data = instance.__dict__["event"][
                                instance_name
                            ]

                            payload = Struct()
                            payload.update(event_data)

                            yield event_manager_pb2.NewEventRequest(
                                type=event_key,
                                instance_name=instance_name,
                                data=payload,
                            )
