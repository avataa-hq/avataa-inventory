from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateObjectWithParamsRequest(_message.Message):
    __slots__ = ["geometry", "p_id", "params", "point_a_id", "point_b_id", "pov", "tmo_id"]
    GEOMETRY_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    POINT_A_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_B_ID_FIELD_NUMBER: _ClassVar[int]
    POV_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    geometry: str
    p_id: int
    params: _containers.RepeatedCompositeFieldContainer[Param]
    point_a_id: int
    point_b_id: int
    pov: str
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., p_id: _Optional[int] = ..., point_a_id: _Optional[int] = ..., point_b_id: _Optional[int] = ..., pov: _Optional[str] = ..., geometry: _Optional[str] = ..., params: _Optional[_Iterable[_Union[Param, _Mapping]]] = ...) -> None: ...

class CreateObjectWithParamsResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Param(_message.Message):
    __slots__ = ["tprm_id", "value"]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    tprm_id: int
    value: str
    def __init__(self, value: _Optional[str] = ..., tprm_id: _Optional[int] = ...) -> None: ...
