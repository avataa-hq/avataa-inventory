from pydantic import parse_obj_as

from services.security_service.routers.models.response_models import (
    PermissionResponse,
)


def transform(raw_objects):
    response = []
    for ro in raw_objects:
        r = ro.__dict__
        parent = ro.parent
        if parent:
            r["root_item_id"] = parent.parent_id
        response.append(r)
    return parse_obj_as(list[PermissionResponse], response)
