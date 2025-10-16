import datetime
import sys
from typing import TypeVar, Type

from fastapi import HTTPException
from psycopg2.errors import UniqueViolation, ForeignKeyViolation
from sqlalchemy import true
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from services.security_service.data.permission import db_admins
from services.security_service.data.permissions.permission_template import (
    PermissionTemplate,
)
from services.security_service.data.utils import (
    get_user_permissions,
    role_prefix,
)
from services.security_service.routers.models.request_models import (
    CreatePermission,
    UpdatePermission,
    CreatePermissions,
)
from services.security_service.routers.utils.recursion import (
    _recursive_existed_up,
    _recursive__merge_up,
    _recursive_existed_down,
    _recursive_merge_down,
)
from services.security_service.security_data_models import UserData

T = TypeVar("T")


def get_permission_name(raw_permission_name: str):
    prefix = f"realm_access.{role_prefix}"
    permission_name = raw_permission_name.replace(prefix, "", 1)
    if permission_name.startswith(role_prefix):
        permission_name = permission_name.replace(role_prefix, "", 1)
    return permission_name


def _get_user_permissions(jwt: UserData | None):
    if not jwt:
        raise HTTPException(status_code=403, detail="Access denied")
    user_permissions = get_user_permissions(jwt)
    user_permissions.append("default")
    return user_permissions


def _check_object_exists(session: Session, main_table, item_id):
    item = session.get(main_table, item_id)
    if not item:
        raise HTTPException(
            status_code=422,
            detail="Object with this ID not found or not available",
        )


def _get_query_available_objects(
    permission_table: Type[T],
    user_permissions: list[str],
    must_be_admin: bool = True,
):
    if not user_permissions:
        raise HTTPException(
            status_code=403, detail="The user does not have access"
        )
    query = select(permission_table.parent_id)
    if not db_admins.intersection(user_permissions):
        query = query.where(permission_table.permission.in_(user_permissions))
        if must_be_admin:
            query = query.where(permission_table.admin == true())
    return query


def __add_root_permission(
    objects: list[PermissionTemplate],
    root_permission_id: int,
    root_permission_name: str,
):
    for obj in objects:
        obj.root_permission_id = root_permission_id
        obj.permission_name = root_permission_name


def get_all_permissions(session: Session, permission_table: Type[T]) -> list[T]:
    query = select(permission_table)
    permissions = session.execute(query).scalars().all()
    return permissions


def get_permissions(
    session: Session, permission_table: Type[T], parent_id: int
) -> list[T]:
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table,
        user_permissions=user_permissions,
        must_be_admin=False,
    )

    query = select(permission_table).where(
        permission_table.parent_id == parent_id,
        permission_table.parent_id.in_(subquery),
    )
    permissions = session.execute(query).scalars().all()
    print(4, datetime.datetime.now())
    return permissions


def create_permission(
    session: Session,
    permission_table: Type[T],
    item: CreatePermission,
    main_table,
    recursive_action_down: dict[str, bool] | None = None,
    recursive_action_up: dict[str, bool] | None = None,
):
    # check
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    is_admin = len(db_admins.intersection(user_permissions)) > 0
    if not is_admin and item.permission not in user_permissions:
        raise HTTPException(
            status_code=404,
            detail="You can only assign roles from the list of roles available to you",
        )

    query = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    ).where(permission_table.parent_id == item.parent_id)
    available_objects = session.execute(query).scalars().all()
    if not available_objects and not is_admin:
        raise HTTPException(
            status_code=404, detail="Parent element not found or access denied"
        )

    _check_object_exists(session, main_table, item.parent_id)

    permission_name = get_permission_name(item.permission)

    # add
    db_item = permission_table(**item.dict(), permission_name=permission_name)
    session.add(db_item)
    try:
        session.flush()
        session.refresh(db_item)
        item_id = db_item.id
        if recursive_action_down:
            perm_down = _recursive_merge_down(
                main_table,
                permission_table,
                item_id=item.parent_id,
                session=session,
                actions=recursive_action_down,
                permission=item.permission,
            )
            if perm_down:
                __add_root_permission(
                    perm_down, db_item.id, root_permission_name=permission_name
                )
                session.add_all(perm_down)
        if recursive_action_up:
            perm_up = _recursive__merge_up(
                main_table,
                permission_table,
                item_id=item.parent_id,
                session=session,
                actions=recursive_action_up,
                permission=item.permission,
            )
            if perm_up:
                __add_root_permission(
                    perm_up, db_item.id, root_permission_name=permission_name
                )
                session.add_all(perm_up)

        session.commit()
    except IntegrityError as e:
        print(e, file=sys.stderr)
        error_msgs = {
            ForeignKeyViolation: "Object with this ID not found or not available",
            UniqueViolation: "An entry already exists for the given permission and object.",
        }
        default_msg = "An unexpected error occurred in the database. Please notify the system administrator"
        error_msg = error_msgs.get(type(e.orig), default_msg)
        raise HTTPException(status_code=422, detail=error_msg)

    return item_id


def create_permissions(
    session: Session,
    permission_table: Type[T],
    items: CreatePermissions,
    main_table,
    recursive_action_down: dict[str, bool] | None = None,
    recursive_action_up: dict[str, bool] | None = None,
):
    ids = []
    try:
        item_main_data = items.dict(exclude={"permission"})
        for permission in items.permission:
            item_main_data["permission"] = permission
            item = CreatePermission(**item_main_data)
            item_id = create_permission(
                session=session,
                permission_table=permission_table,
                item=item,
                main_table=main_table,
                recursive_action_down=recursive_action_down,
                recursive_action_up=recursive_action_up,
            )
            ids.append(item_id)
    except HTTPException as e:
        print(e, file=sys.stderr)
        for item_id in ids:
            delete_object(
                session=session,
                permission_table=permission_table,
                item_id=item_id,
                main_table=main_table,
                recursive_drop_down=True,
                recursive_drop_up=True,
            )
        raise e
    return ids


def update_permission(
    session: Session,
    permission_table: Type[T],
    item: UpdatePermission,
    item_id: int,
    main_table,
    recursive_action_down: dict[str, bool] | None = None,
    recursive_action_up: dict[str, bool] | None = None,
):
    if len(item.get_actions()) == 0:
        raise HTTPException(status_code=422, detail="No field changed")
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    )
    query = select(permission_table).where(
        permission_table.id == item_id, permission_table.parent_id.in_(subquery)
    )
    db_item: PermissionTemplate = session.execute(query).scalar_one_or_none()

    if not db_item:
        raise HTTPException(
            status_code=404, detail="Element not found or access denied"
        )
    if db_item.root_permission_id:
        raise HTTPException(
            status_code=422,
            detail=f"For editing, use the main element of the rule with ID {db_item.root_permission_id}",
        )

    db_item.update_from_dict(item.dict(exclude_unset=True))
    session.add(db_item)
    if recursive_action_down:
        perm_down = _recursive_merge_down(
            main_table,
            permission_table,
            item_id=db_item.parent_id,
            session=session,
            actions=recursive_action_down,
            permission=db_item.permission,
        )
        if perm_down:
            session.add_all(perm_down)
    if recursive_action_up:
        perm_up = _recursive__merge_up(
            main_table,
            permission_table,
            item_id=db_item.parent_id,
            session=session,
            actions=recursive_action_up,
            permission=db_item.permission,
        )
        if perm_up:
            session.add_all(perm_up)
    session.commit()
    session.refresh(db_item)
    return item_id


def delete_object(
    session: Session,
    permission_table: Type[T],
    item_id: int,
    main_table,
    recursive_drop_down: bool = False,
    recursive_drop_up: bool = False,
):
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    )
    query = select(permission_table).where(
        permission_table.id == item_id, permission_table.parent_id.in_(subquery)
    )
    db_item = session.execute(query).scalar_one_or_none()
    if not db_item:
        raise HTTPException(
            status_code=404, detail="Element not found or access denied"
        )
    if db_item.root_permission_id:
        raise HTTPException(
            status_code=422,
            detail=f"For editing, use the main element of the rule with ID {db_item.root_permission_id}",
        )

    session.delete(db_item)
    if recursive_drop_down:
        perm_down = _recursive_existed_down(
            main_table,
            permission_table,
            item_id=db_item.parent_id,
            session=session,
            permission=db_item.permission,
        )
        if perm_down:
            for p in perm_down:
                session.delete(p)
    if recursive_drop_up:
        perm_up = _recursive_existed_up(
            main_table,
            permission_table,
            item_id=db_item.parent_id,
            session=session,
            permission=db_item.permission,
        )
        if perm_up:
            for p in perm_up:
                session.delete(p)
    session.commit()


def delete_objects(
    session: Session,
    permission_table: Type[T],
    item_ids: list[int],
    main_table,
    recursive_drop_down: bool = False,
    recursive_drop_up: bool = False,
):
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    )
    query = select(permission_table).where(
        permission_table.id.in_(item_ids),
        permission_table.parent_id.in_(subquery),
    )
    db_items = session.execute(query).scalars().all()
    if len(db_items) != len(item_ids):
        raise HTTPException(
            status_code=404, detail="Elements not found or access denied"
        )
    # do not combine the next 2 cycles into one. Since this will affect the consistency of the removal
    for db_item in db_items:
        if db_item.root_permission_id:
            raise HTTPException(
                status_code=422,
                detail="For editing, use the main element of the rule",
            )
    for item_id in item_ids:
        delete_object(
            session=session,
            permission_table=permission_table,
            item_id=item_id,
            main_table=main_table,
            recursive_drop_down=recursive_drop_down,
            recursive_drop_up=recursive_drop_up,
        )
