from config import security_config
from config.kafka_config import KAFKA_TURN_ON

from services.security_service.implementation.disabled import DisabledSecurity
from services.security_service.implementation.keycloak import (
    Keycloak,
    KeycloakInfo,
)
from services.security_service.implementation.mixed import (
    OpaJwtRaw,
    OpaJwtParsed,
)
from services.security_service.implementation.utils.user_info_cache import (
    UserInfoCache,
)
from services.security_service.security_interface import SecurityInterface


class SecurityFactory:
    BASE_SCOPES: dict[str, str] = {
        "profile": "Read claims that represent basic profile information",
        "openid": "OpenID Connect scope",
    }

    def get(self, security_type: str) -> SecurityInterface:
        match security_type.upper():
            case "KEYCLOAK":
                return self._get_keycloak()
            case "OPA-JWT-RAW":
                return self._get_opa_jwt_raw()
            case "OPA-JWT-PARSED":
                return self._get_opa_jwt_parsed()
            case "KEYCLOAK-INFO":
                return self._get_keycloak_info()
            case _:
                return self._get_disabled()

    @staticmethod
    def _get_disabled() -> SecurityInterface:
        return DisabledSecurity()

    @staticmethod
    def __import_listeners():
        if KAFKA_TURN_ON:
            from services.security_service.kafka import listener  # noqa
        from services.security_service.data import listener  # noqa

    def _get_keycloak(self) -> SecurityInterface:
        self.__import_listeners()
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {**self.BASE_SCOPES}

        return Keycloak(
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_opa_jwt_raw(self) -> SecurityInterface:
        self.__import_listeners()
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {**self.BASE_SCOPES}

        return OpaJwtRaw(
            opa_url=security_config.OPA_URL,
            policy_path=security_config.OPA_POLICY_PATH,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_opa_jwt_parsed(self) -> SecurityInterface:
        self.__import_listeners()
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {**self.BASE_SCOPES}

        return OpaJwtParsed(
            opa_url=security_config.OPA_URL,
            policy_path=security_config.OPA_POLICY_PATH,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_keycloak_info(self) -> SecurityInterface:
        self.__import_listeners()
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {**self.BASE_SCOPES}

        cache = UserInfoCache()
        cache_user_info_url = security_config.SECURITY_MIDDLEWARE_URL
        return KeycloakInfo(
            cache=cache,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
            cache_user_info_url=cache_user_info_url,
        )


security = SecurityFactory().get(security_config.SECURITY_TYPE)
