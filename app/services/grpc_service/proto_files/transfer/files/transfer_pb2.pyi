from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ListPermission(_message.Message):
    __slots__ = ["permission"]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    permission: _containers.RepeatedCompositeFieldContainer[Permission]
    def __init__(self, permission: _Optional[_Iterable[_Union[Permission, _Mapping]]] = ...) -> None: ...

class Permission(_message.Message):
    __slots__ = ["active", "admin", "create", "delete", "id", "parent_id", "permission", "permission_name", "read", "root_permission_id", "update"]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    ADMIN_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_NAME_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    ROOT_PERMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    active: bool
    admin: bool
    create: bool
    delete: bool
    id: int
    parent_id: int
    permission: str
    permission_name: str
    read: bool
    root_permission_id: int
    update: bool
    def __init__(self, id: _Optional[int] = ..., parent_id: _Optional[int] = ..., permission: _Optional[str] = ..., permission_name: _Optional[str] = ..., root_permission_id: _Optional[int] = ..., read: bool = ..., delete: bool = ..., active: bool = ..., create: bool = ..., update: bool = ..., admin: bool = ...) -> None: ...
