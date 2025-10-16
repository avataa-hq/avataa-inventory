from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Document(_message.Message):
    __slots__ = ["mo_id"]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    mo_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_id: _Optional[_Iterable[int]] = ...) -> None: ...
