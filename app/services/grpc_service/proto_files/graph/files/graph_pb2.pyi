from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InMOsByMoIds(_message.Message):
    __slots__ = ["mo_ids"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class InMOsByTMOid(_message.Message):
    __slots__ = ["chunk_size", "keep_mo_without_prm", "mo_filter_by", "offset", "prm_filter_by", "tmo_id"]
    CHUNK_SIZE_FIELD_NUMBER: _ClassVar[int]
    KEEP_MO_WITHOUT_PRM_FIELD_NUMBER: _ClassVar[int]
    MO_FILTER_BY_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    PRM_FILTER_BY_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    chunk_size: int
    keep_mo_without_prm: bool
    mo_filter_by: str
    offset: int
    prm_filter_by: str
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., mo_filter_by: _Optional[str] = ..., prm_filter_by: _Optional[str] = ..., keep_mo_without_prm: bool = ..., chunk_size: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class InPRMsByPRMIds(_message.Message):
    __slots__ = ["prm_ids"]
    PRM_IDS_FIELD_NUMBER: _ClassVar[int]
    prm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, prm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class InTmoByMoId(_message.Message):
    __slots__ = ["mo_id"]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    mo_id: int
    def __init__(self, mo_id: _Optional[int] = ...) -> None: ...

class InTmoId(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class InTmoIds(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tmo_id: _Optional[_Iterable[int]] = ...) -> None: ...

class InTprmId(_message.Message):
    __slots__ = ["tprm_id"]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    tprm_id: int
    def __init__(self, tprm_id: _Optional[int] = ...) -> None: ...

class InTprmIds(_message.Message):
    __slots__ = ["tprm_ids"]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class MO(_message.Message):
    __slots__ = ["active", "geometry", "id", "label", "latitude", "longitude", "model", "name", "p_id", "params", "point_a_id", "point_b_id", "pov", "status", "tmo_id", "version"]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    POINT_A_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_B_ID_FIELD_NUMBER: _ClassVar[int]
    POV_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    active: bool
    geometry: str
    id: int
    label: str
    latitude: float
    longitude: float
    model: str
    name: str
    p_id: int
    params: _containers.RepeatedCompositeFieldContainer[PRM]
    point_a_id: int
    point_b_id: int
    pov: str
    status: str
    tmo_id: int
    version: int
    def __init__(self, tmo_id: _Optional[int] = ..., p_id: _Optional[int] = ..., id: _Optional[int] = ..., name: _Optional[str] = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., pov: _Optional[str] = ..., geometry: _Optional[str] = ..., model: _Optional[str] = ..., active: bool = ..., point_a_id: _Optional[int] = ..., point_b_id: _Optional[int] = ..., status: _Optional[str] = ..., version: _Optional[int] = ..., params: _Optional[_Iterable[_Union[PRM, _Mapping]]] = ..., label: _Optional[str] = ...) -> None: ...

class OutGetTMOTree(_message.Message):
    __slots__ = ["nodes"]
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[TreeNode]
    def __init__(self, nodes: _Optional[_Iterable[_Union[TreeNode, _Mapping]]] = ...) -> None: ...

class OutMOsByMoIds(_message.Message):
    __slots__ = ["mos"]
    MOS_FIELD_NUMBER: _ClassVar[int]
    mos: _containers.RepeatedCompositeFieldContainer[MO]
    def __init__(self, mos: _Optional[_Iterable[_Union[MO, _Mapping]]] = ...) -> None: ...

class OutMOsStream(_message.Message):
    __slots__ = ["mo"]
    MO_FIELD_NUMBER: _ClassVar[int]
    mo: _containers.RepeatedCompositeFieldContainer[MO]
    def __init__(self, mo: _Optional[_Iterable[_Union[MO, _Mapping]]] = ...) -> None: ...

class OutPRMsByPRMIds(_message.Message):
    __slots__ = ["prms"]
    PRMS_FIELD_NUMBER: _ClassVar[int]
    prms: _containers.RepeatedCompositeFieldContainer[PRM]
    def __init__(self, prms: _Optional[_Iterable[_Union[PRM, _Mapping]]] = ...) -> None: ...

class OutTmoId(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class OutTmoIds(_message.Message):
    __slots__ = ["tmo_ids"]
    TMO_IDS_FIELD_NUMBER: _ClassVar[int]
    tmo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tmo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class OutTprms(_message.Message):
    __slots__ = ["tprms"]
    TPRMS_FIELD_NUMBER: _ClassVar[int]
    tprms: _containers.RepeatedCompositeFieldContainer[TPRM]
    def __init__(self, tprms: _Optional[_Iterable[_Union[TPRM, _Mapping]]] = ...) -> None: ...

class PRM(_message.Message):
    __slots__ = ["id", "mo_id", "tprm_id", "value", "version"]
    ID_FIELD_NUMBER: _ClassVar[int]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    id: int
    mo_id: int
    tprm_id: int
    value: str
    version: int
    def __init__(self, tprm_id: _Optional[int] = ..., mo_id: _Optional[int] = ..., value: _Optional[str] = ..., id: _Optional[int] = ..., version: _Optional[int] = ...) -> None: ...

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
    creation_date: _timestamp_pb2.Timestamp
    description: str
    field_value: str
    group: str
    id: int
    modification_date: _timestamp_pb2.Timestamp
    modified_by: str
    multiple: bool
    name: str
    prm_link_filter: str
    required: bool
    returnable: bool
    tmo_id: int
    val_type: str
    version: int
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., val_type: _Optional[str] = ..., multiple: bool = ..., required: bool = ..., returnable: bool = ..., constraint: _Optional[str] = ..., prm_link_filter: _Optional[str] = ..., group: _Optional[str] = ..., tmo_id: _Optional[int] = ..., id: _Optional[int] = ..., field_value: _Optional[str] = ..., created_by: _Optional[str] = ..., modified_by: _Optional[str] = ..., creation_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modification_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., version: _Optional[int] = ...) -> None: ...

class TreeNode(_message.Message):
    __slots__ = ["child", "created_by", "creation_date", "description", "geometry_type", "global_uniqueness", "icon", "id", "label", "latitude", "lifecycle_process_definition", "line_type", "longitude", "materialize", "minimize", "modification_date", "modified_by", "name", "p_id", "points_constraint_by_tmo", "primary", "severity_id", "status", "version", "virtual"]
    CHILD_FIELD_NUMBER: _ClassVar[int]
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
    child: _containers.RepeatedCompositeFieldContainer[TreeNode]
    created_by: str
    creation_date: _timestamp_pb2.Timestamp
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
    modification_date: _timestamp_pb2.Timestamp
    modified_by: str
    name: str
    p_id: int
    points_constraint_by_tmo: _containers.RepeatedScalarFieldContainer[int]
    primary: _containers.RepeatedScalarFieldContainer[int]
    severity_id: int
    status: int
    version: int
    virtual: bool
    def __init__(self, name: _Optional[str] = ..., p_id: _Optional[int] = ..., icon: _Optional[str] = ..., description: _Optional[str] = ..., virtual: bool = ..., global_uniqueness: bool = ..., lifecycle_process_definition: _Optional[str] = ..., geometry_type: _Optional[str] = ..., materialize: bool = ..., points_constraint_by_tmo: _Optional[_Iterable[int]] = ..., child: _Optional[_Iterable[_Union[TreeNode, _Mapping]]] = ..., id: _Optional[int] = ..., minimize: bool = ..., created_by: _Optional[str] = ..., modified_by: _Optional[str] = ..., latitude: _Optional[int] = ..., longitude: _Optional[int] = ..., creation_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modification_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., primary: _Optional[_Iterable[int]] = ..., severity_id: _Optional[int] = ..., status: _Optional[int] = ..., version: _Optional[int] = ..., line_type: _Optional[str] = ..., label: _Optional[_Iterable[int]] = ...) -> None: ...
