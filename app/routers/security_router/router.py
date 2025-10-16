import datetime

from fastapi import APIRouter, Depends, Path, Body
from sqlmodel import Session
from database import get_session
from models import TMO, MO, TPRM

from services.security_service.data.permissions.inventory import (
    TMOPermission,
    MOPermission,
    TPRMPermission,
)
from services.security_service.routers.models.request_models import (
    CreatePermission,
    UpdatePermission,
    CreatePermissions,
)
from services.security_service.routers.models.response_models import (
    PermissionResponse,
)
from services.security_service.routers.utils.functions import (
    get_all_permissions,
    get_permissions,
    create_permission,
    create_permissions,
    update_permission,
    delete_objects,
    delete_object,
)
from services.security_service.routers.utils.utils import transform
from services.security_service.session.add_security import PREFIX

security_router = APIRouter(prefix=f"{PREFIX}", tags=["Security"])


@security_router.get(
    path="/object_type/", response_model=list[PermissionResponse]
)
def get_all_object_types_permissions(session: Session = Depends(get_session)):
    raw_objects = get_all_permissions(
        session=session, permission_table=TMOPermission
    )
    return transform(raw_objects)


@security_router.get(
    path="/object_type/{tmo_id}", response_model=list[PermissionResponse]
)
def get_object_type_permissions(
    tmo_id: int = Path(...), session: Session = Depends(get_session)
):
    raw_objects = get_permissions(
        session=session, permission_table=TMOPermission, parent_id=tmo_id
    )
    return transform(raw_objects)


@security_router.post(path="/object_type/", status_code=201)
def create_object_type_permission(
    item: CreatePermission,
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = item.get_actions()
    recursive_action_up: dict[str, bool] | None = {"read": True}
    return create_permission(
        session=session,
        permission_table=TMOPermission,
        item=item,
        main_table=TMO,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.post(path="/object_type/multiple", status_code=201)
def create_object_type_permissions(
    items: CreatePermissions,
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = items.get_actions()
    recursive_action_up: dict[str, bool] | None = {"read": True}
    return create_permissions(
        session=session,
        permission_table=TMOPermission,
        items=items,
        main_table=TMO,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.patch(path="/object_type/{id}", status_code=204)
def update_object_type_permission(
    id_: int = Path(..., alias="id"),
    item: UpdatePermission = Body(...),
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = item.get_actions()
    recursive_action_up: dict[str, bool] | None = None
    return update_permission(
        session=session,
        permission_table=TMOPermission,
        item=item,
        item_id=id_,
        main_table=TMO,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.delete(path="/object_type/multiple", status_code=204)
def delete_object_type_permissions(
    id_: list[int] = Body(..., alias="ids", min_items=1),
    session: Session = Depends(get_session),
):
    recursive_drop_down: bool = True
    recursive_drop_up: bool = True
    return delete_objects(
        session=session,
        permission_table=TMOPermission,
        item_ids=id_,
        main_table=TMO,
        recursive_drop_down=recursive_drop_down,
        recursive_drop_up=recursive_drop_up,
    )


@security_router.delete(path="/object_type/{id}", status_code=204)
def delete_object_type_permission(
    id_: int = Path(..., alias="id"), session: Session = Depends(get_session)
):
    recursive_drop_down: bool = True
    recursive_drop_up: bool = True
    return delete_object(
        session=session,
        permission_table=TMOPermission,
        item_id=id_,
        main_table=TMO,
        recursive_drop_down=recursive_drop_down,
        recursive_drop_up=recursive_drop_up,
    )


@security_router.get(path="/objects/", response_model=list[PermissionResponse])
def get_all_objects_permissions(session: Session = Depends(get_session)):
    raw_objects = get_all_permissions(
        session=session, permission_table=MOPermission
    )
    return transform(raw_objects)


@security_router.get(
    path="/objects/{mo_id}", response_model=list[PermissionResponse]
)
def get_object_permissions(
    mo_id: int = Path(...), session: Session = Depends(get_session)
):
    raw_objects = get_permissions(
        session=session, permission_table=MOPermission, parent_id=mo_id
    )
    print(5, datetime.datetime.now())
    response = transform(raw_objects)
    print(6, datetime.datetime.now())
    return response


@security_router.post(path="/objects/multiple", status_code=201)
def create_object_permissions(
    items: CreatePermissions,
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = items.get_actions()
    recursive_action_up: dict[str, bool] | None = {"read": True}
    return create_permissions(
        session=session,
        permission_table=MOPermission,
        items=items,
        main_table=MO,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.post(path="/objects/", status_code=201)
def create_object_permission(
    item: CreatePermission,
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = item.get_actions()
    recursive_action_up: dict[str, bool] | None = {"read": True}
    return create_permission(
        session=session,
        permission_table=MOPermission,
        item=item,
        main_table=MO,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.patch(path="/objects/{id}", status_code=204)
def update_object_permission(
    id_: int = Path(..., alias="id"),
    item: UpdatePermission = Body(...),
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = item.get_actions()
    recursive_action_up: dict[str, bool] | None = None
    return update_permission(
        session=session,
        permission_table=MOPermission,
        item=item,
        item_id=id_,
        main_table=MO,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.delete(path="/objects/multiple", status_code=204)
def delete_object_permissions(
    id_: list[int] = Body(..., alias="ids", min_items=1),
    session: Session = Depends(get_session),
):
    recursive_drop_down: bool = True
    recursive_drop_up: bool = True
    return delete_objects(
        session=session,
        permission_table=MOPermission,
        item_ids=id_,
        main_table=MO,
        recursive_drop_down=recursive_drop_down,
        recursive_drop_up=recursive_drop_up,
    )


@security_router.delete(path="/objects/{id}", status_code=204)
def delete_object_permission(
    id_: int = Path(..., alias="id"), session: Session = Depends(get_session)
):
    recursive_drop_down: bool = True
    recursive_drop_up: bool = True
    return delete_object(
        session=session,
        permission_table=MOPermission,
        item_id=id_,
        main_table=MO,
        recursive_drop_down=recursive_drop_down,
        recursive_drop_up=recursive_drop_up,
    )


@security_router.get("/", response_model=list[PermissionResponse])
def get_all_param_type_permissions(session: Session = Depends(get_session)):
    raw_objects = get_all_permissions(
        session=session, permission_table=TPRMPermission
    )
    return transform(raw_objects)


@security_router.get(
    "/param_type/{tprm_id}", response_model=list[PermissionResponse]
)
def get_param_type_permissions(
    tprm_id: int = Path(...), session: Session = Depends(get_session)
):
    raw_objects = get_permissions(
        session=session, permission_table=TPRMPermission, parent_id=tprm_id
    )
    return transform(raw_objects)


@security_router.post("/param_type/multiple", status_code=201)
def create_param_type_permissions(
    items: CreatePermissions,
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = items.get_actions()
    recursive_action_up: dict[str, bool] | None = {"read": True}
    return create_permissions(
        session=session,
        permission_table=TPRMPermission,
        items=items,
        main_table=TPRM,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.post("/param_type/", status_code=201)
def create_param_type_permission(
    item: CreatePermission, session: Session = Depends(get_session)
):
    recursive_action_down: dict[str, bool] | None = item.get_actions()
    recursive_action_up: dict[str, bool] | None = {"read": True}
    return create_permission(
        session=session,
        permission_table=TPRMPermission,
        item=item,
        main_table=TPRM,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.patch("/param_type/{id}", status_code=204)
def update_param_type_permission(
    id_: int = Path(..., alias="id"),
    item: UpdatePermission = Body(...),
    session: Session = Depends(get_session),
):
    recursive_action_down: dict[str, bool] | None = item.get_actions()
    recursive_action_up: dict[str, bool] | None = None
    return update_permission(
        session=session,
        permission_table=TPRMPermission,
        item=item,
        item_id=id_,
        main_table=TPRM,
        recursive_action_down=recursive_action_down,
        recursive_action_up=recursive_action_up,
    )


@security_router.delete("/param_type/multiple", status_code=204)
def delete_param_type_permissions(
    id_: list[int] = Body(..., alias="ids", min_items=1),
    session: Session = Depends(get_session),
):
    recursive_drop_down: bool = True
    recursive_drop_up: bool = True
    return delete_objects(
        session=session,
        permission_table=TPRMPermission,
        item_ids=id_,
        main_table=TPRM,
        recursive_drop_down=recursive_drop_down,
        recursive_drop_up=recursive_drop_up,
    )


@security_router.delete("/param_type/{id}", status_code=204)
def delete_param_type_permission(
    id_: int = Path(..., alias="id"), session: Session = Depends(get_session)
):
    recursive_drop_down: bool = True
    recursive_drop_up: bool = True
    return delete_object(
        session=session,
        permission_table=TPRMPermission,
        item_id=id_,
        main_table=TPRM,
        recursive_drop_down=recursive_drop_down,
        recursive_drop_up=recursive_drop_up,
    )
