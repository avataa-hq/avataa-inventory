from fastapi import HTTPException
from sqlalchemy import event, true, false, or_
from sqlalchemy.orm import with_loader_criteria

from sqlalchemy.orm import ORMExecuteState, Session

from services.security_service.data.permission import db_permissions, db_admins
from services.security_service.data.utils import get_user_permissions


@event.listens_for(Session, "do_orm_execute")
def selection(orm_execute_state: ORMExecuteState):
    """READ"""
    if orm_execute_state.is_select:
        select_listener(orm_execute_state)


@event.listens_for(Session, "after_flush")
def after_flush(session, flush_context):
    """WRITE"""
    jwt = session.info.get("jwt", None)
    if not jwt:
        return
    user_permissions = get_user_permissions(jwt)
    if not user_permissions:
        raise HTTPException(
            status_code=403, detail="Access permissions missing"
        )
    for new in session.new:
        filter_ = db_permissions.get(new.__class__, None)
        if not filter_:
            continue

        permissions = []
        for user_permission in user_permissions:
            permission = filter_(
                parent_id=new.id,
                permission=user_permission,
                create=True,
                read=True,
                update=True,
                delete=True,
                admin=True,
            )
            permissions.append(permission)
        session.add_all(permissions)


def select_listener(orm_execute_state):
    statement = orm_execute_state.statement
    jwt = orm_execute_state.session.info.get("jwt", None)
    if not jwt:
        return
    user_permissions = get_user_permissions(jwt)
    user_permissions.append("default")
    user_permissions = tuple(user_permissions)
    if not user_permissions:
        raise HTTPException(
            status_code=403, detail="Access permissions missing"
        )
    if set(user_permissions) & db_admins:
        return
    if orm_execute_state.session.info.get("disable_security", False):
        orm_execute_state.session.info["disable_security"] = False
        return
    action = orm_execute_state.session.info.get("action", None)
    statement = add_filter(
        statement, orm_execute_state.session, user_permissions, action
    )
    orm_execute_state.statement = statement


def add_filter(statement, session, user_permissions, action):
    for from_ in statement.froms:
        permissions = db_permissions.get(from_.name, None)
        if not permissions:
            continue
        if not isinstance(permissions, list):
            permissions = [permissions]
        for permission in permissions:
            subquery = session.query(permission.security.parent_id).filter(
                permission.security.permission.in_(user_permissions)
            )

            attrs = []
            if action:
                if not isinstance(action, list):
                    action = [action]
                for i in action:
                    attr = getattr(permission.security, i)
                    attrs.append(attr == true())
            else:
                attr = false()
                attrs.append(attr == true())

            subquery = subquery.filter(or_(*attrs))

            statement = statement.options(
                with_loader_criteria(
                    permission.main,
                    getattr(permission.main, permission.column).in_(subquery),
                    include_aliases=True,
                )
            )

    return statement
