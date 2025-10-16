from enum import Enum

two_way_mo_link_val_type_name = "two-way link"
enum_val_type_name = "enum"


class ErrorHandlingType(str, Enum):
    RAISE_ERROR = "RAISE_ERROR"
    PROCESS_CLEARED = "PROCESS_CLEARED"
    ONLY_CHECKING = "ONLY_CHECKING"
