import grpc

from config.grpc_config import ZEEBE_GRPC_HOST, ZEEBE_GRPC_PORT
from models import GeometryType
from services.grpc_service.proto_files.severity.files import (
    zeebe_severity_pb2_grpc,
    zeebe_severity_pb2,
)


def lifecycle_process_definition_validator(value):
    if value:
        parts = value.split(":")

        if len(parts) != 2:
            raise ValueError(
                "Lifecycle process definition need 2 parts: bpmn process id and bpmn version",
            )

        bpmn_process_id, version = parts

        not_valid_string = (
            bpmn_process_id == ""
            or bpmn_process_id.isspace()
            or bpmn_process_id.isdigit()
        )

        if not_valid_string:
            raise ValueError("BPMN process ID have to be string")

        if not version.isdigit():
            raise ValueError("BPMN version  need to be integer")

        with grpc.insecure_channel(
            f"{ZEEBE_GRPC_HOST}:{ZEEBE_GRPC_PORT}"
        ) as channel:
            stub = zeebe_severity_pb2_grpc.SeverityStub(channel)
            message = (
                zeebe_severity_pb2.CheckLifecycleProcessDefinitionExistsRequest(
                    lifecycle_process_definition=bpmn_process_id,
                    version=int(version),
                )
            )
            response = stub.CheckLifecycleProcessDefinitionExists(message)
            if not response.exists:
                raise ValueError(
                    "Requested BPMN process ID with requested version does not exist"
                )

    return value


def geometry_type_validator(value):
    if value not in GeometryType._member_names_:
        raise ValueError(
            f"Geometry type have to be on of {GeometryType._member_names_}"
        )

    return value
