import enum

RESULT_PREVIEW_FILE_NAME = "preview file.xlsx"
RESULT_EXPORT_FILE_NAME = "export_data"

# ERRORS
EMPTY_VALUE_IN_REQUIRED = "empty_values_in_required"
NOT_MULTIPLE_VALUE = "not_multiple_value"
NOT_VALID_VALUE_BY_CONSTRAINT = "not_valid_value_by_constraint"
NOT_VALID_VALUE_TYPE = "not_valid_value_type"
NOT_VALID_ATTR_VALUE_TYPE = "not_valid_attribute_value_type"
NOT_EXISTS_OBJECTS = "not_exists_objects"
PARENT_CANT_BE_SET = "parents_can_not_be_set"
POINT_CANT_BE_SET = "point_can_not_be_set"
NOT_VALID_BY_PRM_LINK_FILTER = "not_valid_by_prm_link_filter"
NOT_VALID_VALUE = "not_valid_value"
DUPLICATED_VALUES_IN_PRIMARY = "duplicated_primary_value"
DUPLICATED_OBJECT_NAMES = "duplicated_object_names"
NOT_CONCRETE_NAME = "not_concrete_name"

# SEQUENCE_ERRORS
SEQUENCE_LESS_THAN_0 = "sequence_less_than_0"
SEQUENCE_LESS_THAN_SEQ_LENGTH = "sequence_less_than_sequence_length"
SEQUENCE_FOR_EXISTS_OBJECT = "sequence_for_exists_object"
DUPLICATED_SEQUENCE = "duplicated_sequence"

reason_status_to_message = {
    EMPTY_VALUE_IN_REQUIRED: "Values in required TPRM`s must be full",
    NOT_MULTIPLE_VALUE: "Current TPRM is multiple, so it has to store list of values",
    NOT_VALID_VALUE_BY_CONSTRAINT: "Current TPRM has constraint, but this values doesn't match it",
    NOT_VALID_VALUE_TYPE: "Every TPRM has own value type, but this value doesn't match it",
    NOT_VALID_ATTR_VALUE_TYPE: "Every attribute has own value type, but this value doesn't match it",
    NOT_EXISTS_OBJECTS: "You try to add not exists object",
    PARENT_CANT_BE_SET: "Parent can be set, only, if current TMO has parent TMO, and your parent "
    "MO is inside parent TMO",
    POINT_CANT_BE_SET: "Point value can be set, only, if this values matches with your point constraint in TMO",
    NOT_VALID_BY_PRM_LINK_FILTER: "This link to PRM can't be set because of prm link filter",
    SEQUENCE_LESS_THAN_0: "Sequence value can't be less than 0",
    SEQUENCE_LESS_THAN_SEQ_LENGTH: "Sequence value can't be less than sequence length",
    SEQUENCE_FOR_EXISTS_OBJECT: "Sequence can be create with object, it can't be as updated value",
    DUPLICATED_SEQUENCE: "This sequence value is duplicated",
    DUPLICATED_OBJECT_NAMES: "This object name appears more than once at indexes: {}",
    DUPLICATED_VALUES_IN_PRIMARY: "Values in primary column must be unique. You have duplicated values there",
    NOT_VALID_VALUE: "Value can't be set, because it doesn't match with TPRM preferences",
    NOT_CONCRETE_NAME: "Object name is ambiguous! Provide more concrete name!",
}


def get_reason_message(reason_key, *args):
    message_template = reason_status_to_message.get(reason_key, "Unknown")
    return message_template.format(*args)


def get_primary_reason(row_id, reason):
    return f"Row with id {row_id} can't be process, because primary value not valid: {reason}"


XLSX_FORMAT = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

COLUMN_WITH_ORIGINAL_INDEXES = "original_indexes"
COMBINED_NAMES_COLUMN = "combined names"
MO_ID_COLUMN = "mo_id"
SERVICE_COLUMNS = {
    COMBINED_NAMES_COLUMN,
    COLUMN_WITH_ORIGINAL_INDEXES,
    MO_ID_COLUMN,
}


class WarningStatuses(enum.Enum):
    ok = "OK"
    warning = "WARNING"
    error = "ERROR"


def get_cell_format(workbook):
    # yellow background
    return workbook.add_format({"bold": True, "bg_color": "#FFFF00"})
