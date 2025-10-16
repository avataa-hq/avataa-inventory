from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MOPermission(_message.Message):
    __slots__ = ["admin", "create", "delete", "parent_id", "permission", "permission_name", "read", "root_permission_id", "update"]
    ADMIN_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_NAME_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    ROOT_PERMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    admin: bool
    create: bool
    delete: bool
    parent_id: int
    permission: str
    permission_name: str
    read: bool
    root_permission_id: int
    update: bool
    def __init__(self, read: bool = ..., update: bool = ..., create: bool = ..., delete: bool = ..., permission: _Optional[str] = ..., admin: bool = ..., parent_id: _Optional[int] = ..., root_permission_id: _Optional[int] = ..., permission_name: _Optional[str] = ...) -> None: ...

class MOPermissions(_message.Message):
    __slots__ = ["mo_permissions"]
    MO_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    mo_permissions: _containers.RepeatedCompositeFieldContainer[MOPermission]
    def __init__(self, mo_permissions: _Optional[_Iterable[_Union[MOPermission, _Mapping]]] = ...) -> None: ...

class RequestPermissionsForMO(_message.Message):
    __slots__ = ["get_permissions"]
    GET_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    get_permissions: bool
    def __init__(self, get_permissions: bool = ...) -> None: ...
