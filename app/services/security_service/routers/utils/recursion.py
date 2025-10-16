from sqlalchemy import select
from sqlmodel import Session

parent_column = "p_id"


def _get_items_recursive_down(session: Session, main_table, item_id):
    if not hasattr(main_table, parent_column):
        return
    p_id = getattr(main_table, parent_column)
    current_item = (
        session.query(main_table.id, p_id)
        .filter(p_id == item_id)
        .cte("cte", recursive=True)
    )
    bottom_item = session.query(main_table.id, p_id).join(
        current_item, p_id == current_item.c.id
    )
    recursive_query = current_item.union(bottom_item)
    query = session.query(recursive_query)
    child_items = session.execute(query).scalars().all()
    return child_items


def get_items_recursive_up(session: Session, main_table, instance_id):
    if not hasattr(main_table, parent_column):
        return

    p_id = getattr(main_table, parent_column)
    current_item = (
        session.query(main_table.id, p_id)
        .filter(main_table.id == instance_id)
        .cte("cte", recursive=True)
    )

    top_item = session.query(main_table.id, p_id).join(
        current_item, main_table.id == getattr(current_item.c, parent_column)
    )

    recursive_query = current_item.union(top_item)
    query = session.query(recursive_query)
    parent_items = session.execute(query).scalars().all()
    parent_items.remove(instance_id)
    return parent_items


def _get_merged_permissions(
    items_id: list[int],
    permission_table,
    actions: dict[str, bool],
    permission: str,
    session: Session,
):
    if not items_id:
        return

    all_possible_permissions = {}
    for parent_item in items_id:
        new_permission = permission_table(
            **actions, parent_id=parent_item, permission=permission
        )
        all_possible_permissions[parent_item] = new_permission

    query = select(permission_table).filter(
        permission_table.parent_id.in_(items_id),
        permission_table.permission == permission,
    )
    existed_permissions_result = session.execute(query).scalars().all()
    existed_permissions = {i.parent_id: i for i in existed_permissions_result}

    result = []
    for key, possible_permission in all_possible_permissions.items():
        if key in existed_permissions:
            updated_item = existed_permissions[key]
            updated_item.update_from_dict(actions)
            result.append(updated_item)
        else:
            result.append(possible_permission)
    return result


def _recursive_merge_down(
    main_table,
    permission_table,
    item_id,
    session: Session,
    permission: str,
    actions: dict[str, bool],
):
    child_items = _get_items_recursive_down(
        session=session, main_table=main_table, item_id=item_id
    )
    merged_permissions = _get_merged_permissions(
        items_id=child_items,
        permission_table=permission_table,
        actions=actions,
        permission=permission,
        session=session,
    )
    return merged_permissions


def _recursive__merge_up(
    main_table,
    permission_table,
    item_id,
    session: Session,
    permission: str,
    actions: dict[str, bool],
):
    parent_items = get_items_recursive_up(
        session=session, main_table=main_table, instance_id=item_id
    )
    merged_permissions = _get_merged_permissions(
        items_id=parent_items,
        permission_table=permission_table,
        actions=actions,
        permission=permission,
        session=session,
    )
    return merged_permissions


def _recursive_existed_down(
    main_table, permission_table, item_id, session: Session, permission: str
):
    child_items = _get_items_recursive_down(
        session=session, main_table=main_table, item_id=item_id
    )
    query = select(permission_table).filter(
        permission_table.parent_id.in_(child_items),
        permission_table.permission == permission,
    )
    existed_permissions_result = session.execute(query).scalars().all()
    return existed_permissions_result


def _recursive_existed_up(
    main_table, permission_table, item_id, session: Session, permission: str
):
    parent_items = get_items_recursive_up(
        session=session, main_table=main_table, instance_id=item_id
    )
    query = select(permission_table).filter(
        permission_table.parent_id.in_(parent_items),
        permission_table.permission == permission,
    )
    existed_permissions_result = session.execute(query).scalars().all()
    return existed_permissions_result
