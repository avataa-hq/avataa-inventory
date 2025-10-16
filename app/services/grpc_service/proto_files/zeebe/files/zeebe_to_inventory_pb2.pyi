from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InGetTMOIdsByMoIds(_message.Message):
    __slots__ = ["mo_ids"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class InReadChildObjectTypes(_message.Message):
    __slots__ = ["parent_id"]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    parent_id: int
    def __init__(self, parent_id: _Optional[int] = ...) -> None: ...

class InReadObjectTypeParamTypes(_message.Message):
    __slots__ = ["group", "id", "tprm_ids"]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    group: str
    id: int
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, id: _Optional[int] = ..., group: _Optional[str] = ..., tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class InReadObjectTypes(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, id: _Optional[_Iterable[int]] = ...) -> None: ...

class InReadObjects(_message.Message):
    __slots__ = ["active", "identifiers_instead_of_values", "limit", "name", "obj_id", "object_type_id", "offset", "order_by_asc", "order_by_tprms_id", "p_id", "query", "with_parameters"]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIERS_INSTEAD_OF_VALUES_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    OBJ_ID_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_ASC_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_TPRMS_ID_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    WITH_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    active: bool
    identifiers_instead_of_values: bool
    limit: int
    name: str
    obj_id: _containers.RepeatedScalarFieldContainer[int]
    object_type_id: int
    offset: int
    order_by_asc: _containers.RepeatedScalarFieldContainer[bool]
    order_by_tprms_id: _containers.RepeatedScalarFieldContainer[int]
    p_id: int
    query: str
    with_parameters: bool
    def __init__(self, query: _Optional[str] = ..., object_type_id: _Optional[int] = ..., p_id: _Optional[int] = ..., name: _Optional[str] = ..., obj_id: _Optional[_Iterable[int]] = ..., with_parameters: bool = ..., active: bool = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., order_by_tprms_id: _Optional[_Iterable[int]] = ..., order_by_asc: _Optional[_Iterable[bool]] = ..., identifiers_instead_of_values: bool = ...) -> None: ...

class InUpdateObjectType(_message.Message):
    __slots__ = ["id", "object_type", "reset_parameters"]
    ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESET_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    id: int
    object_type: TMOUpdate
    reset_parameters: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[int] = ..., object_type: _Optional[_Union[TMOUpdate, _Mapping]] = ..., reset_parameters: _Optional[_Iterable[str]] = ...) -> None: ...

class OutGetTMOIdsByMoIds(_message.Message):
    __slots__ = ["mapper"]
    class MapperEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    MAPPER_FIELD_NUMBER: _ClassVar[int]
    mapper: _containers.ScalarMap[int, int]
    def __init__(self, mapper: _Optional[_Mapping[int, int]] = ...) -> None: ...

class OutMOArray(_message.Message):
    __slots__ = ["array"]
    ARRAY_FIELD_NUMBER: _ClassVar[int]
    array: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, array: _Optional[_Iterable[str]] = ...) -> None: ...

class OutTMOArray(_message.Message):
    __slots__ = ["tmo"]
    TMO_FIELD_NUMBER: _ClassVar[int]
    tmo: _containers.RepeatedCompositeFieldContainer[TMO]
    def __init__(self, tmo: _Optional[_Iterable[_Union[TMO, _Mapping]]] = ...) -> None: ...

class OutTPRMArray(_message.Message):
    __slots__ = ["array"]
    ARRAY_FIELD_NUMBER: _ClassVar[int]
    array: _containers.RepeatedCompositeFieldContainer[TPRM]
    def __init__(self, array: _Optional[_Iterable[_Union[TPRM, _Mapping]]] = ...) -> None: ...

class TMO(_message.Message):
    __slots__ = ["child_count", "created_by", "creation_date", "description", "geometry_type", "global_uniqueness", "icon", "id", "label", "latitude", "lifecycle_process_definition", "line_type", "longitude", "materialize", "minimize", "modification_date", "modified_by", "name", "p_id", "points_constraint_by_tmo", "primary", "severity_id", "status", "version", "virtual"]
    CHILD_COUNT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_UNIQUENESS_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_PROCESS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    LINE_TYPE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    MATERIALIZE_FIELD_NUMBER: _ClassVar[int]
    MINIMIZE_FIELD_NUMBER: _ClassVar[int]
    MODIFICATION_DATE_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_BY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    POINTS_CONSTRAINT_BY_TMO_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    VIRTUAL_FIELD_NUMBER: _ClassVar[int]
    child_count: int
    created_by: str
    creation_date: str
    description: str
    geometry_type: str
    global_uniqueness: bool
    icon: str
    id: int
    label: _containers.RepeatedScalarFieldContainer[int]
    latitude: int
    lifecycle_process_definition: str
    line_type: str
    longitude: int
    materialize: bool
    minimize: bool
    modification_date: str
    modified_by: str
    name: str
    p_id: int
    points_constraint_by_tmo: _containers.RepeatedScalarFieldContainer[int]
    primary: _containers.RepeatedScalarFieldContainer[int]
    severity_id: int
    status: int
    version: int
    virtual: bool
    def __init__(self, id: _Optional[int] = ..., version: _Optional[int] = ..., name: _Optional[str] = ..., p_id: _Optional[int] = ..., latitude: _Optional[int] = ..., longitude: _Optional[int] = ..., status: _Optional[int] = ..., icon: _Optional[str] = ..., description: _Optional[str] = ..., child_count: _Optional[int] = ..., virtual: bool = ..., global_uniqueness: bool = ..., primary: _Optional[_Iterable[int]] = ..., created_by: _Optional[str] = ..., modified_by: _Optional[str] = ..., creation_date: _Optional[str] = ..., modification_date: _Optional[str] = ..., lifecycle_process_definition: _Optional[str] = ..., geometry_type: _Optional[str] = ..., severity_id: _Optional[int] = ..., materialize: bool = ..., points_constraint_by_tmo: _Optional[_Iterable[int]] = ..., minimize: bool = ..., line_type: _Optional[str] = ..., label: _Optional[_Iterable[int]] = ...) -> None: ...

class TMOUpdate(_message.Message):
    __slots__ = ["description", "geometry_type", "global_uniqueness", "icon", "label", "latitude", "lifecycle_process_definition", "line_type", "longitude", "name", "p_id", "primary", "severity_id", "status", "version", "virtual"]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_UNIQUENESS_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_PROCESS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    LINE_TYPE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    VIRTUAL_FIELD_NUMBER: _ClassVar[int]
    description: str
    geometry_type: str
    global_uniqueness: bool
    icon: str
    label: _containers.RepeatedScalarFieldContainer[int]
    latitude: int
    lifecycle_process_definition: str
    line_type: str
    longitude: int
    name: str
    p_id: int
    primary: _containers.RepeatedScalarFieldContainer[int]
    severity_id: int
    status: int
    version: int
    virtual: bool
    def __init__(self, version: _Optional[int] = ..., name: _Optional[str] = ..., p_id: _Optional[int] = ..., latitude: _Optional[int] = ..., longitude: _Optional[int] = ..., status: _Optional[int] = ..., icon: _Optional[str] = ..., description: _Optional[str] = ..., virtual: bool = ..., global_uniqueness: bool = ..., primary: _Optional[_Iterable[int]] = ..., lifecycle_process_definition: _Optional[str] = ..., severity_id: _Optional[int] = ..., geometry_type: _Optional[str] = ..., line_type: _Optional[str] = ..., label: _Optional[_Iterable[int]] = ...) -> None: ...

class TPRM(_message.Message):
    __slots__ = ["constraint", "created_by", "creation_date", "description", "field_value", "group", "id", "modification_date", "modified_by", "multiple", "name", "prm_link_filter", "required", "returnable", "tmo_id", "val_type", "version"]
    CONSTRAINT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    FIELD_VALUE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    MODIFICATION_DATE_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_BY_FIELD_NUMBER: _ClassVar[int]
    MULTIPLE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRM_LINK_FILTER_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    RETURNABLE_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    VAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    constraint: str
    created_by: str
    creation_date: str
    description: str
    field_value: str
    group: str
    id: int
    modification_date: str
    modified_by: str
    multiple: bool
    name: str
    prm_link_filter: str
    required: bool
    returnable: bool
    tmo_id: int
    val_type: str
    version: int
    def __init__(self, description: _Optional[str] = ..., multiple: bool = ..., required: bool = ..., returnable: bool = ..., constraint: _Optional[str] = ..., prm_link_filter: _Optional[str] = ..., group: _Optional[str] = ..., id: _Optional[int] = ..., version: _Optional[int] = ..., creation_date: _Optional[str] = ..., modification_date: _Optional[str] = ..., name: _Optional[str] = ..., val_type: _Optional[str] = ..., tmo_id: _Optional[int] = ..., created_by: _Optional[str] = ..., modified_by: _Optional[str] = ..., field_value: _Optional[str] = ...) -> None: ...
