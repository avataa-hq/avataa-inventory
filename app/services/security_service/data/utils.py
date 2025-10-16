from services.security_service.security_data_models import UserData

role_prefix = "__"


def get_user_permissions(jwt: UserData) -> list[str]:
    permissions = []
    if jwt.realm_access:
        permissions.extend(
            [
                f"{jwt.realm_access.name}.{r}"
                for r in jwt.realm_access.roles
                if r.startswith(role_prefix)
            ]
        )
    if jwt.resource_access:
        for resource_access in jwt.resource_access:
            permissions.extend(
                [
                    f"{resource_access.name}.{r}"
                    for r in resource_access.roles
                    if r.startswith(role_prefix)
                ]
            )
    return permissions
