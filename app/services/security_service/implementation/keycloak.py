import asyncio
import logging
from typing import Optional, Dict

import aiohttp
import jwt
from aiohttp import ClientConnectionError, ClientResponseError, InvalidURL
from fastapi import HTTPException
from fastapi.requests import Request
from fastapi.security import OAuth2AuthorizationCodeBearer

from services.security_service.implementation.utils.user_info_cache import (
    UserInfoCacheInterface,
)
from services.security_service.security_data_models import UserData
from services.security_service.security_interface import SecurityInterface


class Keycloak(OAuth2AuthorizationCodeBearer, SecurityInterface):
    def __init__(
        self,
        keycloak_public_url: str,
        authorization_url: str,
        token_url: str,
        refresh_url: Optional[str] = None,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
        options: Optional[dict] = None,
    ):
        super(Keycloak, self).__init__(
            authorizationUrl=authorization_url,
            tokenUrl=token_url,
            refreshUrl=refresh_url,
            scheme_name=scheme_name,
            scopes=scopes,
            description=description,
            auto_error=auto_error,
        )
        self.keycloak_public_url = keycloak_public_url
        self._public_key = None
        if not options:
            options = {
                "verify_signature": True,
                "verify_aud": False,
                "verify_exp": True,
            }
        self._options = options

    async def _get_public_key(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.keycloak_public_url,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        raise HTTPException(
                            status_code=503,
                            detail="Token verification service unavailable",
                        )
                    data = await resp.json()
        except ClientConnectionError:
            raise HTTPException(
                status_code=503,
                detail="Failed to connect to token verification service",
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=503,
                detail="Token verification service request timed out",
            )
        except ClientResponseError:
            raise HTTPException(
                status_code=503,
                detail="Error in token verification service response",
            )
        except InvalidURL:
            raise HTTPException(
                status_code=503,
                detail="Invalid URL for token verification service",
            )

        public_key = (
            "-----BEGIN PUBLIC KEY-----\n"
            + data["public_key"]
            + "\n-----END PUBLIC KEY-----"
        )
        return public_key

    async def __call__(self, request: Request) -> UserData:
        token = await super(Keycloak, self).__call__(request)
        user_info = await self._parse_jwt(token=token)
        return UserData.from_jwt(user_info)

    async def _parse_jwt(self, token: str) -> dict:
        if self._public_key is None:
            self._public_key = await self._get_public_key()

        user_info = await self._decode_token(token)
        return user_info

    async def _decode_token(self, token: str):
        try:
            decoded_token = jwt.decode(
                token,
                self._public_key,
                algorithms=["RS256"],
                options=self._options,
            )
        except jwt.PyJWTError as e:
            logging.warning(e)
            raise HTTPException(status_code=403, detail=str(e))
        return decoded_token


class KeycloakInfo(Keycloak):
    INFO_PREFIX = "/protocol/openid-connect/userinfo"

    def __init__(
        self,
        cache: UserInfoCacheInterface | None,
        keycloak_public_url: str,
        authorization_url: str,
        token_url: str,
        refresh_url: Optional[str] = None,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
        options: Optional[dict] = None,
        cache_user_info_url: str | None = None,
    ):
        super(KeycloakInfo, self).__init__(
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
        self.logger = logging.getLogger("KeycloakInfo")
        self.info_url = (
            cache_user_info_url
            or f"{self.keycloak_public_url}{self.INFO_PREFIX}"
        )
        self.cache = cache

    async def __call__(self, request: Request) -> UserData:
        token = await super(Keycloak, self).__call__(request)
        user_info = await self._parse_jwt(token=token)
        additional_data = await self.get_user_info(token=token)
        if additional_data:
            user_info.update(additional_data)
        return UserData.from_jwt(user_info)

    async def get_from_cache(self, token: str) -> dict | None:
        if not self.cache:
            return None
        return self.cache.get(token)

    async def set_in_cache(self, token: str, value: dict) -> None:
        if not self.cache:
            return
        self.cache.set(token, value)

    async def get_from_keycloak(self, token: str) -> dict | None:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.info_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        raise HTTPException(
                            status_code=503,
                            detail="Token verification service unavailable [Keycloak INFO]",
                        )
                    data = await resp.json()
        except ClientConnectionError as e:
            print(e)
            raise HTTPException(
                status_code=503,
                detail="Failed to connect to token verification service [Keycloak INFO]",
            )
        except asyncio.TimeoutError as e:
            print(e)
            raise HTTPException(
                status_code=503,
                detail="Token verification service request timed out [Keycloak INFO]",
            )
        except ClientResponseError as e:
            print(e)
            raise HTTPException(
                status_code=503,
                detail="Error in token verification service response [Keycloak INFO]",
            )
        except InvalidURL as e:
            print(e)
            raise HTTPException(
                status_code=503,
                detail="Invalid URL for token verification service [Keycloak INFO]",
            )
        else:
            return data

    async def get_user_info(self, token: str) -> dict | None:
        cached = await self.get_from_cache(token=token)
        if not cached:
            cached = await self.get_from_keycloak(token=token)
            await self.set_in_cache(token=token, value=cached)
        return cached
