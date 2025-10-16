from typing import ClassVar as _ClassVar, Optional as _Optional

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class NewEventRequest(_message.Message):
    __slots__ = ["type", "instance_name", "data"]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    type: str
    instance_name: str
    data: str
    def __init__(self, type: _Optional[str] = ..., instance_name: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class NewEventResponse(_message.Message):
    __slots__ = ["is_success", "message"]
    IS_SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    is_success: bool
    message: str
    def __init__(self, is_success: bool = ..., message: _Optional[str] = ...) -> None: ...
