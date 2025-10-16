from dataclasses import dataclass
from enum import Enum
from typing import Any

from models import MO, TMO, TPRM, PRM
from services.grpc_service.proto_files.inventory_instances.files import (
    inventory_instances_pb2,
)
from val_types.constants import enum_val_type_name
from val_types.constants import two_way_mo_link_val_type_name

NAME_DELIMITER = "-"

MO_LINK_DELIMITER = "::"

allowed_val_types = [
    "str",
    "int",
    "float",
    "bool",
    "date",
    "datetime",
    "mo_link",
    "prm_link",
    "user_link",
    "formula",
    "sequence",
    "formula",
    two_way_mo_link_val_type_name,
    enum_val_type_name,
]

not_multiple_val_types = {"formula", "sequence", two_way_mo_link_val_type_name}

not_required_val_types = {"prm_link", "sequence", two_way_mo_link_val_type_name}

val_types_with_required_constraint = {
    "prm_link",
    "formula",
    two_way_mo_link_val_type_name,
    enum_val_type_name,
}

val_types_cannot_be_changed_to = {
    "prm_link",
    "mo_link",
    "user_link",
    "sequence",
    two_way_mo_link_val_type_name,
    enum_val_type_name,
}

VALID_DATE_FORMATS = ["%Y-%m-%d"]
VALID_DATETIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]


class AvailableInstances(str, Enum):
    TMO = "TMO"
    TPRM = "TPRM"
    MO = "MO"
    PRM = "PRM"


class ObjEventStatus(Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


class DocumentsEventType(str, Enum):
    DELETED = "DELETED"
    CREATED = "CREATED"


@dataclass
class ModelMessageData:
    class_model: Any
    proto_unit_template: Any
    proto_list_template: Any


MODEL_EQ_MESSAGE: dict[str, ModelMessageData] = {
    "MO": ModelMessageData(
        class_model=MO,
        proto_unit_template=inventory_instances_pb2.MO,
        proto_list_template=inventory_instances_pb2.ListMO,
    ),
    "TMO": ModelMessageData(
        class_model=TMO,
        proto_unit_template=inventory_instances_pb2.TMO,
        proto_list_template=inventory_instances_pb2.ListTMO,
    ),
    "TPRM": ModelMessageData(
        class_model=TPRM,
        proto_unit_template=inventory_instances_pb2.TPRM,
        proto_list_template=inventory_instances_pb2.ListTPRM,
    ),
    "PRM": ModelMessageData(
        class_model=PRM,
        proto_unit_template=inventory_instances_pb2.PRM,
        proto_list_template=inventory_instances_pb2.ListPRM,
    ),
}
