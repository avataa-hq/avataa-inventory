from starlette.requests import Request  # noqa

from services.security_service.security_interface import SecurityInterface
from services.security_service.security_data_models import UserData, ClientRoles


class DisabledSecurity(SecurityInterface):
    async def __call__(self, request: Request) -> UserData:
        return UserData(
            id=None,
            audience=None,
            name="Anonymous",
            preferred_name="Anonymous",
            realm_access=ClientRoles(name="realm_access", roles=["__admin"]),
            resource_access=None,
            groups=None,
        )
