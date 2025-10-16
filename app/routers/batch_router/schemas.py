from dataclasses import dataclass
from typing import Any

from pandas import DataFrame
from enum import Enum


class ExportFileTypes(Enum):
    csv = "csv"
    xlsx = "xlsx"

    @staticmethod
    def values():
        return [member.value for member in ExportFileTypes]


@dataclass
class ResultDataframes:
    updated_mo_prms: DataFrame
    updated_mo_attrs: DataFrame
    created_mo_prms: DataFrame
    created_mo_attrs: DataFrame
    create_mo_and_prm_and_attr: DataFrame
    deleted_object_values: DataFrame


class ErrorProcessor(Enum):
    RAISE = "RAISE"
    COLLECT = "COLLECT"


@dataclass
class BatchPreviewErrorInstance:
    error_value: Any
    index_of_error_value: int
    status: str


@dataclass
class BatchPreviewWarningInstance:
    warning_value: Any
    index_of_error_value: int


@dataclass
class ErrorInstances:
    errors: dict[str, list[BatchPreviewErrorInstance]]
    warnings: dict[str, list[BatchPreviewWarningInstance]]


@dataclass
class ErrorsAndWarnings:
    errors: list[BatchPreviewErrorInstance]
    warnings: list[BatchPreviewWarningInstance]


@dataclass
class BatchImportValidatorResponse:
    result_dataframes: ResultDataframes
    error_instances: ErrorInstances


@dataclass
class DataframesForCreateAndUpdate:
    dataframe_with_data_for_create: DataFrame
    dataframe_with_data_for_update: DataFrame


@dataclass
class ResultFiles:
    updated_mo_params: DataFrame
    updated_mo_attrs: DataFrame
    created_mo_attrs: DataFrame
    deleted_object_values: DataFrame


@dataclass
class ConvertedToListValuesInColumn:
    column_with_converted_values: DataFrame
    errors: list


@dataclass
class ValidatedRequiredColumn:
    validated_dataframe: DataFrame
    errors: list


@dataclass
class ProcessedObjectParams:
    created_mo_prms_temp: DataFrame
    updated_mo_prms_temp: DataFrame
    deleted_object_values_temp: DataFrame


@dataclass
class ProcessedObjectAttributes:
    updated_mo_attrs_temp: DataFrame
    created_mo_attrs_temp: DataFrame
    deleted_object_values_temp: DataFrame
