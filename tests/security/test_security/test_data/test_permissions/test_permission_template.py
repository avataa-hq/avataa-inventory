# from services.security_service.data.permissions.permission_template import PermissionTemplate
#
#
# def test_update_negative():
#     val_dict = {'admin': True, 'create': True, 'delete': True, 'parent_id': 1, 'permission': 'test', 'read': True,
#                 'update': True}
#
#     permission = PermissionTemplate(**val_dict)
#
#     permission.update_from_dict({'delete': False})
#
#     assert permission.to_dict() != val_dict
#
#
# def test_update_positive():
#     val_dict = {'admin': True, 'create': True, 'delete': True, 'parent_id': 1, 'permission': 'test', 'read': True,
#                 'update': True}
#
#     permission = PermissionTemplate(**val_dict)
#
#     permission.update_from_dict({'delete': False})
#     val_dict['delete'] = False
#
#     assert permission.to_dict() == val_dict
