# from services.security_service.data.utils import get_user_permissions
# from services.security_service.security_data_models import UserData
#
#
# def test_get_user_permissions_positive(jwt_admin: UserData):
#     right_roles = {'realm_access.__reader', 'realm_access.__admin'}
#
#     user_permissions = get_user_permissions(jwt_admin)
#     assert right_roles == set(user_permissions)
