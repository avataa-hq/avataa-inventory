from enum import Enum
from services.grpc_service.proto_files.inventory_instances.files.inventory_instances_pb2 import (
    TMO,
    MO,
    PRM,
    TPRM,
)


class ProtoInstances(Enum):
    TMO = TMO
    MO = MO
    TPRM = TPRM
    PRM = PRM
