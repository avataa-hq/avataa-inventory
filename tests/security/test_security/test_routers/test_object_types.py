# from fastapi import HTTPException
#
# from services.security_service.data.permissions.inventory import TMOPermission
# from services.security_service.routers.models.request_models import CreatePermission, UpdatePermission
# from services.security_service.routers.models.response_models import PermissionResponse
# from routers.security_router.router import get_object_type_permissions, create_object_type_permission, \
#     update_object_type_permission, delete_object_type_permission
# import pytest
#
#
# def test_get_object_type_permissions_negative(session):
#     tmo_id = 100500
#     with pytest.raises(HTTPException):
#         get_object_type_permissions(tmo_id=tmo_id, session=session)
#
#
# def test_get_object_type_permissions_default_positive(session, jwt_admin, jwt_reader, jwt_other):
#     expected = [
#         PermissionResponse(permission_id=3,
#                            parent_id=1,
#                            root_item_id=3,
#                            root_permission_id=1,
#                            permission='default',
#                            permission_name='default',
#                            create=False,
#                            read=True,
#                            update=False,
#                            delete=False,
#                            admin=False)
#     ]
#     session.info['action'] = 'read'
#     tmo_id = 1
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     result = get_object_type_permissions(tmo_id=tmo_id, session=session)
#     assert result == expected
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     result = get_object_type_permissions(tmo_id=tmo_id, session=session)
#     assert result == expected
#
#     # other
#     session.info['jwt'] = jwt_other
#     result = get_object_type_permissions(tmo_id=tmo_id, session=session)
#     assert result == expected
#
#
# def test_get_object_type_permissions_private_positive(session, jwt_admin, jwt_reader, jwt_other):
#     expected = [
#         PermissionResponse(permission_id=1,
#                            parent_id=3,
#                            root_item_id=None,
#                            root_permission_id=None,
#                            permission='realm_access.__reader',
#                            permission_name='reader',
#                            create=True,
#                            read=True,
#                            update=True,
#                            delete=True,
#                            admin=False)
#     ]
#     session.info['action'] = 'read'
#     tmo_id = 3
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     result = get_object_type_permissions(tmo_id=tmo_id, session=session)
#     assert result == expected
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     result = get_object_type_permissions(tmo_id=tmo_id, session=session)
#     assert result == expected
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         get_object_type_permissions(tmo_id=tmo_id, session=session)
#
#
# # @pytest.mark.skip
# def test_create_object_type_permission_default_negative(session, jwt_admin, jwt_reader, jwt_other):
#     session.info['action'] = 'create'
#
#     item = CreatePermission(parent_id=1, permission='default', create=True, read=True, update=True, delete=True,
#                             admin=True)
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item, session=session)
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item, session=session)
#
#
# # @pytest.mark.skip
# def test_create_object_type_permission(session, jwt_admin, jwt_reader, jwt_other):
#     session.info['action'] = 'create'
#
#     item1 = CreatePermission(parent_id=3, permission='realm_access.__reader', create=True, read=True, update=True,
#                              delete=True, admin=True)
#     item2 = CreatePermission(parent_id=2, permission='realm_access.__new_perm', create=True, read=True, update=True,
#                              delete=True, admin=True)
#     item3 = CreatePermission(parent_id=2, permission='default', create=True, read=True, update=True,
#                              delete=True, admin=True)
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item1, session=session)
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item2, session=session)
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item3, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item1, session=session)
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item2, session=session)
#
#     result = create_object_type_permission(item=item3, session=session)
#     assert result == 4
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item1, session=session)
#     session.rollback()
#
#     result = create_object_type_permission(item=item2, session=session)
#     assert result == 7
#
#     with pytest.raises(HTTPException):
#         create_object_type_permission(item=item3, session=session)
#
# def test_update_object_type_permission(session, jwt_admin, jwt_reader, jwt_other):
#     session.info['action'] = 'update'
#
#     item1 = UpdatePermission(create=True, read=True, update=True, delete=True, admin=True)
#     item2 = UpdatePermission(create=False, read=False, update=False, delete=False, admin=False)
#     item3 = UpdatePermission(create=None, read=None, update=None, delete=None, admin=None)
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=1, item=item1, session=session)
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=2, item=item2, session=session)
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=3, item=item3, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=1, item=item1, session=session)
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=2, item=item2, session=session)
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=3, item=item1, session=session)
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=3, item=item1, session=session)
#     with pytest.raises(HTTPException):
#         update_object_type_permission(id_=2, item=item2, session=session)
#     result = update_object_type_permission(id_=1, item=item1, session=session)
#     assert result == 1
#
#     session.info['disable_security'] = True
#     main_item = session.get(TMOPermission, 1)
#     assert main_item.to_dict(only_actions=True) == item1.get_actions()
#
#     session.info['disable_security'] = True
#     child_item = session.get(TMOPermission, 3)
#     assert child_item.to_dict(only_actions=True) != item1.get_actions()
#
#
# def test_delete_object_type_permission_other(session, jwt_admin, jwt_reader, jwt_other):
#     session.info['action'] = 'delete'
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=1, session=session)
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=2, session=session)
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=3, session=session)
#
#
# def test_delete_object_type_permission_reader(session, jwt_admin, jwt_reader, jwt_other):
#     session.info['action'] = 'delete'
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=2, session=session)
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=3, session=session)
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=1, session=session)
#
#
# def test_delete_object_type_permission_admin(session, jwt_admin, jwt_reader, jwt_other):
#     session.info['action'] = 'delete'
#     # admin
#     session.info['jwt'] = jwt_admin
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=2, session=session)
#     with pytest.raises(HTTPException):
#         delete_object_type_permission(id_=3, session=session)
#
#     delete_object_type_permission(id_=1, session=session)
#     session.info['disable_security'] = True
#     main_item = session.get(TMOPermission, 1)
#     assert main_item is None
#
#     session.info['disable_security'] = True
#     child_item = session.get(TMOPermission, 2)
#     assert child_item is None
#
#     session.info['disable_security'] = True
#     child_item = session.get(TMOPermission, 3)
#     assert child_item is not None
