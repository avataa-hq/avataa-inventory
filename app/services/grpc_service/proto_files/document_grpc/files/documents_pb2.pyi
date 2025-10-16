from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RequestGetObjectDocumentCount(_message.Message):
    __slots__ = ["check"]
    CHECK_FIELD_NUMBER: _ClassVar[int]
    check: bool
    def __init__(self, check: bool = ...) -> None: ...

class ResponseGetObjectDocumentCount(_message.Message):
    __slots__ = ["object_and_documents"]
    class ObjectAndDocumentsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    OBJECT_AND_DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    object_and_documents: _containers.ScalarMap[int, int]
    def __init__(self, object_and_documents: _Optional[_Mapping[int, int]] = ...) -> None: ...
