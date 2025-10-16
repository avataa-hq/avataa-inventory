from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass
class ClientRoles:
    name: str
    roles: List[str]


@dataclass
class UserData:
    id: Optional[str]
    audience: Optional[Union[List[str], str]]
    name: str
    preferred_name: str
    realm_access: Optional[ClientRoles]
    resource_access: Optional[List[ClientRoles]]
    groups: Optional[List[str]]
    session_id: Union[str, None] = None

    @classmethod
    def from_jwt(cls, jwt: dict):
        realm_access = jwt.get("realm_access", None)
        if realm_access is not None:
            realm_access = ClientRoles(
                name="realm_access", roles=realm_access.get("roles", [])
            )
        resource_access = jwt.get("resource_access", None)
        if resource_access is not None:
            resource_access = [
                ClientRoles(name=k, roles=v.get("roles", []))
                for k, v in resource_access.items()
            ]

        return cls(
            id=jwt.get("sub", None),
            audience=jwt.get("aud", None),
            name=f"""{jwt.get("given_name", "")} {jwt.get("family_name", "")}""",
            preferred_name=jwt.get("preferred_username", jwt.get("upn", "")),
            realm_access=realm_access,
            resource_access=resource_access,
            groups=jwt.get("groups", None),
            session_id=jwt.get("sid", None),
        )
