from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetMOByTMOIdForViewRequest(_message.Message):
    __slots__ = ["replace_links", "tmo_id"]
    REPLACE_LINKS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    replace_links: bool
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., replace_links: bool = ...) -> None: ...

class GetMOByTMOIdForViewResponse(_message.Message):
    __slots__ = ["mos_with_params"]
    MOS_WITH_PARAMS_FIELD_NUMBER: _ClassVar[int]
    mos_with_params: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mos_with_params: _Optional[_Iterable[str]] = ...) -> None: ...
