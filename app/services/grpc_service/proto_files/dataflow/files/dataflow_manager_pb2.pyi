from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RequestGetObjectsWithParams(_message.Message):
    __slots__ = ["limit", "offset", "tmo_id", "tprm_names"]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_NAMES_FIELD_NUMBER: _ClassVar[int]
    limit: int
    offset: int
    tmo_id: int
    tprm_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tmo_id: _Optional[int] = ..., tprm_names: _Optional[_Iterable[str]] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class RequestGetTPRMNameToTypeMapper(_message.Message):
    __slots__ = ["columns", "tmo_id"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., columns: _Optional[_Iterable[str]] = ...) -> None: ...

class RequestGetTPRMNamesOfTMO(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class ResponseGetObjectsWithParams(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: str
    def __init__(self, data: _Optional[str] = ...) -> None: ...

class ResponseGetTPRMNameToTypeMapper(_message.Message):
    __slots__ = ["mapper"]
    class MapperEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    MAPPER_FIELD_NUMBER: _ClassVar[int]
    mapper: _containers.ScalarMap[str, str]
    def __init__(self, mapper: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResponseGetTPRMNamesOfTMO(_message.Message):
    __slots__ = ["column"]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    column: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, column: _Optional[_Iterable[str]] = ...) -> None: ...
