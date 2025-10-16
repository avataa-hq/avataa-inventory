from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestBatchExport(_message.Message):
    __slots__ = ["file_type", "prm_type_names", "tmo_id"]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRM_TYPE_NAMES_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    file_type: str
    prm_type_names: _containers.RepeatedScalarFieldContainer[str]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., file_type: _Optional[str] = ..., prm_type_names: _Optional[_Iterable[str]] = ...) -> None: ...

class RequestBatchImport(_message.Message):
    __slots__ = ["content", "tmo_id"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    content: str
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., content: _Optional[str] = ...) -> None: ...

class RequestCreateTMOOrGetInfo(_message.Message):
    __slots__ = ["description", "geometry_type", "global_uniqueness", "icon", "label", "lifecycle_process_definition", "materialize", "name", "p_id"]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_UNIQUENESS_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_PROCESS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    MATERIALIZE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    description: str
    geometry_type: str
    global_uniqueness: bool
    icon: str
    label: _containers.RepeatedScalarFieldContainer[int]
    lifecycle_process_definition: str
    materialize: bool
    name: str
    p_id: int
    def __init__(self, name: _Optional[str] = ..., p_id: _Optional[int] = ..., icon: _Optional[str] = ..., description: _Optional[str] = ..., global_uniqueness: bool = ..., lifecycle_process_definition: _Optional[str] = ..., geometry_type: _Optional[str] = ..., materialize: bool = ..., label: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestCreateTPRMsForTMO(_message.Message):
    __slots__ = ["tmo_id", "tprms"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRMS_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    tprms: _containers.RepeatedCompositeFieldContainer[TPRMInfo]
    def __init__(self, tmo_id: _Optional[int] = ..., tprms: _Optional[_Iterable[_Union[TPRMInfo, _Mapping]]] = ...) -> None: ...

class RequestDeleteAllObjects(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestGetMOAttrsAndTPRMs(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestGetRequiredFields(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestGetTMOLocations(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestGetTMOName(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestGetTPRMNamesByIds(_message.Message):
    __slots__ = ["tprm_ids"]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ResponseBatchExport(_message.Message):
    __slots__ = ["content", "message", "status"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    content: str
    message: str
    status: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class ResponseBatchImport(_message.Message):
    __slots__ = ["message", "status"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message: str
    status: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ResponseCreateTMOOrGetInfo(_message.Message):
    __slots__ = ["message", "name", "status", "tmo_id"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    message: str
    name: str
    status: str
    tmo_id: int
    def __init__(self, status: _Optional[str] = ..., tmo_id: _Optional[int] = ..., name: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ResponseCreateTPRMsForTMO(_message.Message):
    __slots__ = ["message", "status", "tprms"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TPRMS_FIELD_NUMBER: _ClassVar[int]
    message: str
    status: str
    tprms: TPRMNameToId
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ..., tprms: _Optional[_Union[TPRMNameToId, _Mapping]] = ...) -> None: ...

class ResponseDeleteAllObjects(_message.Message):
    __slots__ = ["message", "status"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message: str
    status: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ResponseGetMOAttrsAndTPRMs(_message.Message):
    __slots__ = ["columns"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseGetRequiredFields(_message.Message):
    __slots__ = ["fields"]
    class FieldsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.ScalarMap[int, str]
    def __init__(self, fields: _Optional[_Mapping[int, str]] = ...) -> None: ...

class ResponseGetTMOLocations(_message.Message):
    __slots__ = ["geometry", "name"]
    GEOMETRY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    geometry: str
    name: str
    def __init__(self, name: _Optional[str] = ..., geometry: _Optional[str] = ...) -> None: ...

class ResponseGetTMOName(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ResponseGetTPRMNamesByIds(_message.Message):
    __slots__ = ["mapper"]
    class MapperEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    MAPPER_FIELD_NUMBER: _ClassVar[int]
    mapper: _containers.ScalarMap[int, str]
    def __init__(self, mapper: _Optional[_Mapping[int, str]] = ...) -> None: ...

class TPRMInfo(_message.Message):
    __slots__ = ["constraint", "description", "field_value", "group", "multiple", "name", "prm_link_filter", "required", "returnable", "val_type"]
    CONSTRAINT_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    FIELD_VALUE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    MULTIPLE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRM_LINK_FILTER_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    RETURNABLE_FIELD_NUMBER: _ClassVar[int]
    VAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    constraint: str
    description: str
    field_value: str
    group: str
    multiple: bool
    name: str
    prm_link_filter: str
    required: bool
    returnable: bool
    val_type: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., val_type: _Optional[str] = ..., multiple: bool = ..., required: bool = ..., returnable: bool = ..., constraint: _Optional[str] = ..., prm_link_filter: _Optional[str] = ..., group: _Optional[str] = ..., field_value: _Optional[str] = ...) -> None: ...

class TPRMNameToId(_message.Message):
    __slots__ = ["mapper"]
    class MapperEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    MAPPER_FIELD_NUMBER: _ClassVar[int]
    mapper: _containers.ScalarMap[str, int]
    def __init__(self, mapper: _Optional[_Mapping[str, int]] = ...) -> None: ...
