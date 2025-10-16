from typing import Optional

from fastapi.requests import Request

from services.security_service.data.permission import db_admins
from services.security_service.data.utils import get_user_permissions
from services.security_service.implementation.keycloak import Keycloak
from services.security_service.implementation.opa import OPA
from services.security_service.security_data_models import UserData


class OpaJwtRaw(Keycloak, OPA):
    """
    We get jwt token.
    Redirect request to OPA with encoded token.
    The result from OPA is analyzed
    """

    def __init__(
        self,
        opa_url: str,
        policy_path,
        keycloak_public_url: str,
        authorization_url: str,
        token_url: str,
        refresh_url: Optional[str] = None,
        scheme_name: Optional[str] = None,
        scopes: Optional[dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
    ):
        options = {
            "verify_signature": False,
            "verify_aud": False,
            "verify_exp": False,
        }
        Keycloak.__init__(
            self=self,
            keycloak_public_url=keycloak_public_url,
            authorization_url=authorization_url,
            token_url=token_url,
            refresh_url=refresh_url,
            scheme_name=scheme_name,
            scopes=scopes,
            description=description,
            auto_error=auto_error,
            options=options,
        )
        OPA.__init__(self=self, opa_url=opa_url, policy_path=policy_path)
        self._public_key = ""

    async def __call__(self, request: Request) -> UserData:
        token = await super(Keycloak, self).__call__(request)
        jwt_decoded = await self._parse_jwt(token=token)
        self._check_opa(request)
        return UserData.from_jwt(jwt_decoded)


class OpaJwtParsed(Keycloak, OPA):
    """
    We get jwt token.
    We send a token that has already been decrypted and verified by us.
    The result from OPA is analyzed
    """

    def __init__(
        self,
        opa_url: str,
        policy_path,
        keycloak_public_url: str,
        authorization_url: str,
        token_url: str,
        refresh_url: Optional[str] = None,
        scheme_name: Optional[str] = None,
        scopes: Optional[dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
    ):
        options = {
            "verify_signature": True,
            "verify_aud": False,
            "verify_exp": True,
        }
        Keycloak.__init__(
            self=self,
            keycloak_public_url=keycloak_public_url,
            authorization_url=authorization_url,
            token_url=token_url,
            refresh_url=refresh_url,
            scheme_name=scheme_name,
            scopes=scopes,
            description=description,
            auto_error=auto_error,
            options=options,
        )
        OPA.__init__(self=self, opa_url=opa_url, policy_path=policy_path)

    async def __call__(self, request: Request) -> UserData:
        token = await super(Keycloak, self).__call__(request)
        jwt_decoded = await self._parse_jwt(token)
        user_data = UserData.from_jwt(jwt_decoded)
        user_permissions = get_user_permissions(user_data)
        is_admin = len(db_admins.intersection(user_permissions)) > 0
        if not is_admin:
            self._check_opa(request, data={"jwt": jwt_decoded})
        return user_data
