from dataclasses import dataclass
from enum import Enum
from typing import Union


class SessionDataKeys(Enum):
    NEW = "created_instances"
    DELETED = "deleted_instances"
    DIRTY = "updated_instances"


PARAMETER_TYPE_INSTANCES_CACHE = {}


@dataclass
class AdditionalData:
    user_id: Union[str, None] = None
    session_id: Union[str, None] = None
