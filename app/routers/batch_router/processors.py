import ast
import copy
import io
import json
import pickle
import re
from ast import literal_eval
from collections import Counter
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any, Literal, List, Optional, Union

import numpy as np
import pandas as pd
import xlsxwriter
from fastapi import (
    HTTPException,
    BackgroundTasks,
    Depends,
    Query,
)
from pandas import DataFrame, Series
from sqlalchemy import (
    null as sql_null,
    cast,
    func,
    String,
    and_,
)
from sqlalchemy.orm import aliased
from sqlmodel import Session, select
from xlsxwriter.worksheet import Worksheet

from common.common_constant import NAME_DELIMITER
from database import (
    SQLALCHEMY_LIMIT,
    get_session,
    get_chunked_values_by_sqlalchemy_limit,
)
from functions.functions_dicts import value_convertation_by_val_type
from functions.functions_utils.utils import (
    calculate_by_formula_batch,
    extract_location_data,
    decode_multiple_value,
)
from functions.validation_functions.validation_utils import (
    get_possible_prm_ids_for_internal_link,
    get_possible_prm_ids_for_external_link,
)
from models import PRM, TMO, MO, MOBase, TPRM
from routers.batch_router.constants import (
    EMPTY_VALUE_IN_REQUIRED,
    NOT_MULTIPLE_VALUE,
    NOT_VALID_VALUE_BY_CONSTRAINT,
    NOT_VALID_VALUE_TYPE,
    NOT_VALID_ATTR_VALUE_TYPE,
    NOT_EXISTS_OBJECTS,
    PARENT_CANT_BE_SET,
    SEQUENCE_LESS_THAN_0,
    SEQUENCE_LESS_THAN_SEQ_LENGTH,
    COLUMN_WITH_ORIGINAL_INDEXES,
    DUPLICATED_SEQUENCE,
    SERVICE_COLUMNS,
    MO_ID_COLUMN,
    COMBINED_NAMES_COLUMN,
    WarningStatuses,
    NOT_VALID_BY_PRM_LINK_FILTER,
    get_primary_reason,
    get_reason_message,
    reason_status_to_message,
    DUPLICATED_OBJECT_NAMES,
    NOT_VALID_VALUE,
    NOT_CONCRETE_NAME,
    XLSX_FORMAT,
)
from routers.batch_router.exceptions import (
    RequestedTMOIsVirtual,
    NotAllowedFileType,
    FileReadingException,
    NotUniqueColumnsInFile,
    NotExistsTPRMsInHeader,
    NotAddedRequiredAttributes,
    NotExistsMOAttributes,
    ColumnValuesValidationError,
    DuplicatedMONameInFile,
    SequenceNotImplemented,
    TMONotExists,
)
from routers.batch_router.schemas import (
    ErrorProcessor,
    BatchPreviewErrorInstance,
    BatchPreviewWarningInstance,
    ErrorsAndWarnings,
    DataframesForCreateAndUpdate,
    ProcessedObjectParams,
    ProcessedObjectAttributes,
    ResultFiles,
    ResultDataframes,
    ErrorInstances,
    BatchImportValidatorResponse,
    ValidatedRequiredColumn,
    ExportFileTypes,
    ConvertedToListValuesInColumn,
)
from routers.batch_router.utils import (
    BatchConstantVariables,
    BatchFileConstants,
    BatchErrorAndWarningsCollector,
)
from routers.object_router.utils import (
    TPRMFilterCleaner,
    update_geometry,
)
from routers.parameter_router.schemas import PRMUpdateByMO, PRMCreateByMO
from routers.parameter_type_router.utils import (
    get_list_trpms_by_tmo_and_val_type,
)
from val_types.constants import (
    enum_val_type_name,
    two_way_mo_link_val_type_name,
)
from val_types.two_way_mo_link_val_type.prm.create import (
    check_create_two_way_prms,
)
from val_types.two_way_mo_link_val_type.prm.update import (
    check_update_two_way_prms,
)


class BatchImportValidator(
    BatchConstantVariables, BatchFileConstants, BatchErrorAndWarningsCollector
):
    def __init__(
        self,
        file: bytes,
        session: Session,
        object_type_id: int,
        column_name_mapping: dict,
        delimiter: str,
        file_content_type: str,
        columns_to_drop: list[str] = None,
        raise_errors: ErrorProcessor = False,
    ):
        BatchConstantVariables.__init__(self, columns_to_drop=columns_to_drop)
        BatchFileConstants.__init__(self)
        BatchErrorAndWarningsCollector.__init__(self)

        self._session: Session = session
        self._raise_error_status: str = raise_errors.value

        # FILE INIT VARIABLES
        self._file_content_type = file_content_type
        self._delimiter: str = delimiter
        self._file_in_bytes_view: bytes = file
        # dict[temporary_tprm_name: real_tprm_name]
        self._column_name_mapping: dict[str, str] = column_name_mapping

        # DB INSTANCES
        self._object_type_instance: TMO = session.get(TMO, object_type_id)
        if not self._object_type_instance:
            raise TMONotExists(
                status_code=422,
                detail=f"Object type with id {object_type_id} not found.",
            )

        self._latitude_tprm: str = str(self._object_type_instance.latitude)
        self._longitude_tprm: str = str(self._object_type_instance.longitude)
        self._status_tprm: str = str(self._object_type_instance.status)

    @staticmethod
    def _validate_object_type(object_type: TMO) -> None:
        """
        If TMO is virtual - it can't have objects, so we can't use batch
        """
        if object_type.virtual:
            raise RequestedTMOIsVirtual(
                status_code=400,
                detail=f"TMO ({object_type.name}) is virtual. You can`t create objects "
                f"with TMO.virtual equal to True.",
            )

    def _parent_id_column_converter(
        self, parent_ids: set[int]
    ) -> dict[int, str]:
        parent_ids = [int(i) for i in parent_ids if i is not None]
        parent_name_by_id = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(parent_ids):
            stmt = select(MO.id, MO.name).where(MO.id.in_(chunk))
            chunk_result = self._session.exec(stmt).all()
            parent_name_by_id.update(
                {
                    parent_id: parent_name
                    for parent_id, parent_name in chunk_result
                }
            )
        return parent_name_by_id

    def _get_dataframe_from_file_data(self) -> DataFrame:
        """
        This method converts file data, which
        can be presented by byte to dataframe
        """
        with io.BytesIO(self._file_in_bytes_view) as data:
            try:
                if self._file_content_type == XLSX_FORMAT:
                    self._main_dataframe = pd.read_excel(
                        data,
                        engine="openpyxl",
                        dtype=str,
                        na_values=self._FILE_EMPTY_VALUES,
                        keep_default_na=False,
                    )

                else:
                    self._main_dataframe = pd.read_csv(
                        data,
                        dtype=str,
                        delimiter=self._delimiter,
                        na_values=self._FILE_EMPTY_VALUES,
                        keep_default_na=False,
                    )

                self._main_dataframe = self._main_dataframe.where(
                    cond=pd.notna(self._main_dataframe), other=None
                )

            except BaseException as e:
                raise FileReadingException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                    detail=str(e),
                )

        return self._main_dataframe

    def _validate_file_headers_for_uniqueness(self) -> None:
        columns = self._main_dataframe.columns.values.tolist()

        pattern = re.compile(r".*\..*")
        check_columns = [column for column in columns if pattern.match(column)]

        if len(check_columns) > 0:
            not_unique_col_names = [x.split(".")[0] for x in check_columns]

            if not_unique_col_names:
                raise NotUniqueColumnsInFile(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                    detail=f"Column names must be unique. Not unique columns: {not_unique_col_names}.",
                )

    @staticmethod
    def _get_values_from_dataframe_by_indexes(
        dataframe: DataFrame, column_name: str, indexes_to_search: list[int]
    ):
        return dataframe.loc[indexes_to_search, column_name].values.tolist()

    @staticmethod
    def _get_indexes_by_values_in_dataframe(
        dataframe: DataFrame,
        column_name_with_values: str,
        values_to_search: list | set,
        column_name_with_indexes: str,
    ):
        values_to_search_str = [str(v) for v in values_to_search]

        return dataframe.loc[
            dataframe[column_name_with_values]
            .astype(str)
            .isin(values_to_search_str),
            column_name_with_indexes,
        ].tolist()

    # COLLECT DATA
    def _get_reversed_column_name_mapping(self) -> dict[str, str]:
        """
        After we convert TPRM names to TPRM ids -- we need in return in result file
        TPRM names, not TPRM ids, and if there are temporary mapped TPRM names - we need return them

        So this method created dict with [TPRM_IDS/MO_ATTRs: TPRM_NAMES/MO_ATTRs]
        """
        dataframe_columns = self._main_dataframe.columns

        reversed_column_name_mapping = {
            real_name: temp_name
            for temp_name, real_name in self._column_name_mapping.items()
        }

        for column_name in dataframe_columns:
            if column_name.isdigit():
                tprm_id = column_name
                tprm_instance = self._tprm_instance_by_id.get(int(tprm_id))
                if (
                    tprm_instance
                    and tprm_instance.name in reversed_column_name_mapping
                ):
                    tprm_name = reversed_column_name_mapping[tprm_instance.name]

                else:
                    tprm_name = tprm_instance.name

                self._new_column_name_mapping[tprm_id] = tprm_name

            else:
                self._new_column_name_mapping[column_name] = column_name

        return self._new_column_name_mapping

    def _update_header_of_dataframe_by_tprm_ids(self) -> DataFrame:
        """
        column_name_mapping: dict[temporary_tprm_name: real_tprm_name]

        Because user can create a file with not existed TPRM names in header,
        by adding mapping to request -- we need to convert this temporary name to service view

        TPRM names must be TPRM ids
        """
        # get just tprm names, separated from mo attributes
        requested_tprm_names = [
            column_name
            for column_name in self._main_dataframe.columns
            if column_name not in self._mo_attributes_available_for_batch
        ]

        # in values of column_name_mapping we store real tprm names,
        # so we can get both: mapped by user and real tprm names too
        real_tprm_names = [
            self._column_name_mapping.get(tprm_name, tprm_name)
            for tprm_name in requested_tprm_names
        ]

        if real_tprm_names:
            stmt = select(TPRM).where(
                TPRM.tmo_id == self._object_type_instance.id,
                TPRM.name.in_(real_tprm_names),
            )
            db_tprms: list[TPRM] = self._session.execute(stmt).scalars().all()

            db_tprm_names = {tprm.name for tprm in db_tprms}
            db_tprm_name_and_id: dict[str, str] = {
                tprm.name: str(tprm.id) for tprm in db_tprms
            }

            not_exists_tprm_names = sorted(
                list(set(real_tprm_names).difference(db_tprm_names))
            )

            tprm_ids_in_requested_header = [
                int(tprm_id)
                for tprm_id in not_exists_tprm_names
                if tprm_id.isdigit()
            ]
            if tprm_ids_in_requested_header:
                stmt = select(TPRM.id).where(
                    TPRM.tmo_id == self._object_type_instance.id,
                    TPRM.id.in_(tprm_ids_in_requested_header),
                )
                exists_tprm_ids: list[TPRM] = (
                    self._session.execute(stmt).scalars().all()
                )
                not_exists_tprm_ids = set(
                    tprm_ids_in_requested_header
                ).difference(set(exists_tprm_ids))
                if not_exists_tprm_ids:
                    raise NotExistsTPRMsInHeader(
                        status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                        detail=f"There are TPRM in header, which are not exists: {not_exists_tprm_ids}",
                    )
                not_exists_tprm_names = [
                    column_name
                    for column_name in not_exists_tprm_names
                    if not column_name.isdigit()
                ]

            if not_exists_tprm_names:
                raise NotExistsTPRMsInHeader(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                    detail=f"There are TPRM in header, which are not exists: {not_exists_tprm_names}",
                )

            # dataframe headers must by mo attributes or tprm ids(in string view),
            # so we need to update tprm names by ids, and leave mo attribute names
            new_column_names = []

            for column_name in self._main_dataframe.columns:
                if column_name in self._mo_attributes_available_for_batch:
                    new_column_names.append(column_name)

                elif column_name in self._column_name_mapping:
                    real_tprm_name = self._column_name_mapping[column_name]
                    tprm_id_in_str_view = db_tprm_name_and_id[real_tprm_name]

                    new_column_names.append(tprm_id_in_str_view)

                else:
                    tprm_id_in_str_view = db_tprm_name_and_id[column_name]
                    new_column_names.append(tprm_id_in_str_view)

            self._main_dataframe.columns = new_column_names

        return self._main_dataframe

    @staticmethod
    def _replace_values_in_dataframe(
        dataframe: DataFrame, old_value: Any, new_value: Any
    ) -> DataFrame:
        """
        This method replaces any value in every column in dataframe to new one
        """
        updated_dataframe = dataframe.replace(old_value, new_value)
        return updated_dataframe

    def _get_tprm_instances_by_val_types(self):
        """
        This method collect TPRM instances, depend on TPRM val types used in header if file
        """
        self._sequence_tprm_instances = get_list_trpms_by_tmo_and_val_type(
            session=self._session,
            tmo_id=self._object_type_instance.id,
            val_type="sequence",
        )

        self._sequence_tprms = {
            seq_tprm.id: seq_tprm for seq_tprm in self._sequence_tprm_instances
        }

        if self._sequence_tprms:
            raise SequenceNotImplemented(
                status_code=422,
                detail="Processing sequence values are not implemented in batch yet",
            )

        for tprm in self._tprm_instance_by_id.values():
            if tprm.val_type == "formula":
                self._formula_tprm_ids.append(str(tprm.id))

            if tprm.val_type == "mo_link":
                self._mo_link_tprms[tprm.id] = tprm

            elif tprm.val_type == "prm_link":
                self._prm_link_tprms[tprm.id] = tprm

                if tprm.prm_link_filter:
                    self._prm_link_tprms_with_prm_link_filter[tprm.id] = tprm

            elif tprm.val_type == two_way_mo_link_val_type_name:
                self._two_way_mo_link_tprms.append(tprm)

    def _get_primary_data_from_headers(self) -> list[str]:
        for column in self._main_dataframe.columns:
            if (
                column.isdigit()
                and int(column) in self._object_type_instance.primary
            ):
                self._primary_tprms.append(column)

        return self._primary_tprms

    def _get_mo_attrs_from_headers(self) -> list[str]:
        for column in self._main_dataframe.columns:
            if column not in SERVICE_COLUMNS:
                self._mo_attrs_and_tprms.append(column)

        return self._mo_attrs_and_tprms

    def _get_tprm_instances_by_id(
        self, tprm_ids: list[int] | list[str]
    ) -> dict[int, TPRM]:
        tprm_instances = (
            self._session.execute(select(TPRM).where(TPRM.id.in_(tprm_ids)))
            .scalars()
            .all()
        )

        self._tprm_instance_by_id: dict[int, TPRM] = {
            tprm.id: tprm for tprm in tprm_instances
        }
        return self._tprm_instance_by_id

    def _get_object_names_by_ids(
        self, object_names: Series, object_type: TMO
    ) -> dict[int, str]:
        exists_object_names_with_ids = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(object_names):
            stmt = select(MO.id, MO.name).where(
                MO.name.in_(chunk), MO.tmo_id == object_type.id
            )
            exists_parameters = self._session.exec(stmt).all()

            exists_object_names_with_ids.update(
                {mo_id: mo_name for mo_id, mo_name in exists_parameters}
            )

        return exists_object_names_with_ids

    def _mo_attributes_validation(self, mo_attr: str) -> (list, list):
        values_with_indexes = self._main_dataframe[mo_attr]

        validation_task = ObjectAttributesValidation(
            session=self._session,
            mo_attribute_name=mo_attr,
            tmo_instance=self._object_type_instance,
            values_with_indexes=values_with_indexes,
        )

        error_and_warnings = validation_task.validate()

        return error_and_warnings

    def _tprm_parameters_validation(self, tprm_id: str) -> ErrorsAndWarnings:
        current_tprm_instance = self._tprm_instance_by_id[int(tprm_id)]
        values_with_indexes = self._main_dataframe[tprm_id]

        if current_tprm_instance.multiple:
            validation_task = MultipleTPRMValidation(
                session=self._session,
                tprm_instance=current_tprm_instance,
                values_with_indexes=values_with_indexes,
            )

        else:
            validation_task = SingleTPRMValidation(
                session=self._session,
                tmo_instance=self._object_type_instance,
                tprm_instance=current_tprm_instance,
                values_with_indexes=values_with_indexes,
            )

        error_and_warnings = validation_task.validate()

        return error_and_warnings

    def _validate_duplicates_in_sequences_if_exist_for_batch_preview(self):
        """
        We can't validate sequence tprm one by one, because, we need to see
        every tprm and connections between them
        """
        sequence_tprm_instances = list(self._sequence_tprms.values())

        for sequence_tprm in sequence_tprm_instances:
            if str(sequence_tprm.id) in self._main_dataframe.columns:
                subset = [str(sequence_tprm.id)]
                if sequence_tprm.constraint:
                    subset.append(sequence_tprm.constraint)

                filtered_df = self._main_dataframe[
                    pd.notna(self._main_dataframe[str(sequence_tprm.id)])
                ]
                duplicates = filtered_df[
                    filtered_df.duplicated(subset, keep=False)
                ].values.tolist()

                if duplicates:
                    duplicate_indexes = filtered_df[
                        filtered_df.duplicated(subset, keep=False)
                    ].index.tolist()
                    error_instances = (
                        self._format_error_structure_for_batch_preview(
                            error_values=duplicates,
                            error_indexes=duplicate_indexes,
                            error_status=DUPLICATED_SEQUENCE,
                        )
                    )

                    match self._raise_error_status:
                        case ErrorProcessor.RAISE.value:
                            statuses_by_index = {
                                error.index_of_error_value: error.status
                                for error in error_instances
                            }
                            column_name = self._new_column_name_mapping[
                                str(sequence_tprm)
                            ]
                            raise ColumnValuesValidationError(
                                status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                                detail=f"There are error data in column {column_name}. "
                                f"Error statuses by index: {statuses_by_index}",
                            )

                    self._error_row_with_reasons[str(sequence_tprm.id)].extend(
                        error_instances
                    )

        return self._error_row_with_reasons

    def _validate_file_values(self):
        """
        A main file, that validate dataframe data
        """
        error_indexes_to_delete = set()
        warning_indexes_to_delete = set()

        for column in self._mo_attrs_and_tprms:
            self._dominant_types.append(
                calculate_dominant_type(self._main_dataframe[column])
            )

            if column.isdigit():
                errors_and_warnings: ErrorsAndWarnings = (
                    self._tprm_parameters_validation(tprm_id=column)
                )
            else:
                errors_and_warnings: ErrorsAndWarnings = (
                    self._mo_attributes_validation(mo_attr=column)
                )

            # if primary value is warning or error -- object can't be created or determined
            if errors_and_warnings.errors:
                match self._raise_error_status:
                    case ErrorProcessor.RAISE.value:
                        statuses_by_index = {
                            error.index_of_error_value: error.status
                            for error in errors_and_warnings.errors
                        }
                        column_name = self._new_column_name_mapping[column]
                        raise ColumnValuesValidationError(
                            status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                            detail=f"There are error data in column {column_name}. Error statuses by index:"
                            f"{statuses_by_index}",
                        )

                self._error_row_with_reasons[column].extend(
                    errors_and_warnings.errors
                )

                if column in self._primary_tprms:
                    error_indexes_to_delete = [
                        error.index_of_error_value
                        for error in errors_and_warnings.errors
                    ]

            if errors_and_warnings.warnings:
                self._warning_indexes[column] = errors_and_warnings.warnings

                if column in self._primary_tprms:
                    match self._raise_error_status:
                        case ErrorProcessor.RAISE.value:
                            statuses_by_index = {
                                error.index_of_error_value: error.status
                                for error in errors_and_warnings.errors
                            }
                            column_name = self._new_column_name_mapping[column]
                            raise ColumnValuesValidationError(
                                status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                                detail=f"There are error data in primary column {column_name}. "
                                f"Error statuses by index: {statuses_by_index}",
                            )

                    warning_indexes_to_delete = [
                        warning_value.index_of_error_value
                        for warning_value in errors_and_warnings.warnings
                    ]

        if error_indexes_to_delete:
            self._main_dataframe.drop(
                list(error_indexes_to_delete), inplace=True
            )

        if warning_indexes_to_delete:
            self._main_dataframe.drop(
                list(warning_indexes_to_delete), inplace=True
            )

        self._validate_duplicates_in_sequences_if_exist_for_batch_preview()

    def _convert_tprm_values(
        self, parameter_type_instance: TPRM, values_to_convert: list[Any]
    ) -> list[Any]:
        if parameter_type_instance.multiple:
            conversion_task = MultipleTPRMValuesConverter(
                session=self._session,
                values_to_convert=values_to_convert,
                tprm_instance=parameter_type_instance,
            )

        else:
            conversion_task = SingleTPRMValuesConverter(
                session=self._session,
                values_to_convert=values_to_convert,
                tprm_instance=parameter_type_instance,
            )

        converted_values = conversion_task.convert()

        return converted_values

    def _convert_mo_attribute_values(
        self,
        mo_attribute_name: str,
        values_to_convert: list[Any],
        object_type_instance: TMO,
    ):
        conversion_task = ObjectAttributeValuesConverter(
            session=self._session,
            values_to_convert=values_to_convert,
            attribute_name=mo_attribute_name,
            object_type_instance=object_type_instance,
        )

        converted_values = conversion_task.convert()

        return converted_values

    def _convert_dataframe_values_to_db_view(self) -> DataFrame:
        """
        Because dataframe determine every row value as string,
        so we need to convert these values by linked "val types"
        """
        for column in self._mo_attrs_and_tprms:
            column_values = self._main_dataframe[column].values.tolist()

            if column.isdigit():
                current_tprm_instance = self._tprm_instance_by_id[int(column)]
                converted_values = self._convert_tprm_values(
                    parameter_type_instance=current_tprm_instance,
                    values_to_convert=column_values,
                )

            else:
                converted_values = self._convert_mo_attribute_values(
                    mo_attribute_name=column,
                    values_to_convert=column_values,
                    object_type_instance=self._object_type_instance,
                )

            self._main_dataframe[column] = converted_values
        return self._main_dataframe

    @staticmethod
    def _get_parameter_names_from_formula(constraint: str) -> set:
        nodes = {}
        try:
            if ";" in constraint:
                str_pattern = re.compile(r"'.+'")
                conditions: list[str] = constraint.split(";")
                if_state = conditions[0]
                regex = re.compile(
                    r"(?:if|elif) (.*) (==|!=|>|>=|<=|<) (?:and|or (==|!=|>|>=|<=|<))?(.*)? then (.*)"
                )
                if regex.fullmatch(if_state):
                    expressions = regex.findall(if_state)[0]
                    if not str_pattern.fullmatch(expressions[0]):
                        nodes[expressions[0]] = ast.parse(
                            expressions[0], "<string>", mode="eval"
                        )
                else:
                    raise HTTPException(
                        status_code=422, detail="Could not parse formula"
                    )
                else_state = conditions[-1]
                regex = re.compile(r" else (.+)")
                if else_state:
                    match_result = regex.fullmatch(else_state)
                    if match_result:
                        expressions = regex.findall(else_state)
                        first_expression = expressions[0]
                        if not str_pattern.fullmatch(first_expression):
                            nodes[first_expression] = ast.parse(
                                first_expression, "<string>", mode="eval"
                            )
                    else:
                        raise HTTPException(
                            status_code=422, detail="Could not parse formula"
                        )
                for i in range(1, len(conditions) - 1):
                    regex = re.compile(
                        r" (?:if|elif) (.*) (==|!=|>|>=|<=|<) (?:and|or (==|!=|>|>=|<=|<))?(.*)? then (.*)"
                    )
                    if regex.fullmatch(conditions[i]):
                        expressions = regex.findall(conditions[i])[0]
                        if not str_pattern.fullmatch(expressions[0]):
                            nodes[expressions[0]] = ast.parse(
                                expressions[0], "<string>", mode="eval"
                            )
                    else:
                        raise ValueError()
            else:
                nodes[constraint] = ast.parse(
                    constraint, "<string>", mode="eval"
                )
        except SyntaxError:
            raise HTTPException(
                status_code=422, detail="Could not parse formula"
            )

        parameter_type_names = []
        function_names = []
        for formula, node in nodes.items():
            parameter_type_names.extend(
                [
                    nd.slice.value
                    for nd in ast.walk(node)
                    if isinstance(nd, ast.Subscript)
                    and isinstance(nd.slice, ast.Constant)
                ]
            )

            for nd in ast.walk(node):
                if isinstance(nd, ast.Subscript) and isinstance(
                    nd.value, ast.Name
                ):
                    function_names.append(nd.value.id)
                elif isinstance(nd, ast.Subscript) and isinstance(
                    nd.value, ast.Call
                ):
                    function_names.append(nd.value.func.attr)

        parameter_type_names = [
            n
            for n, f in zip(parameter_type_names, function_names)
            if f == "parameter"
        ]
        inner_max_names = [
            n
            for n, f in zip(parameter_type_names, function_names)
            if f == "INNER_MAX"
        ]
        return set(list({*parameter_type_names, *inner_max_names}))

    def _process_formula_data(self) -> DataFrame:
        """
        This method get every formula TPRM and calculate every row (which equal to MO)
        for every formula TPRM
        """
        not_valid_formula_indexes = []

        dataframe_without_prm_links = copy.deepcopy(self._main_dataframe)

        prm_link_column_names = [
            str(prm_link_id) for prm_link_id in self._prm_link_tprms.keys()
        ]
        dataframe_without_prm_links.drop(columns=prm_link_column_names)

        needed_tprm_names = set()
        for tprm_id in self._formula_tprm_ids:
            tprm_instance = self._tprm_instance_by_id[int(tprm_id)]

            needed_tprm_names.update(
                self._get_parameter_names_from_formula(
                    constraint=tprm_instance.constraint
                )
            )

        query = select(TPRM).where(
            TPRM.name.in_(needed_tprm_names),
            TPRM.multiple != True,  # noqa
            TPRM.tmo_id == self._object_type_instance.id,
        )
        tprms = self._session.exec(query).all()
        tprm_by_name: dict[str, TPRM] = {t.name: t for t in tprms}

        if self._formula_tprm_ids:
            for index, row in dataframe_without_prm_links.iterrows():
                row = {column_name: value for column_name, value in row.items()}

                for column in self._formula_tprm_ids:
                    formula_tprm = self._tprm_instance_by_id[int(column)]

                    try:
                        value = calculate_by_formula_batch(
                            session=self._session,
                            formula_tprm=formula_tprm,
                            prm_data=row,
                            tprms_from_formula_by_name=tprm_by_name,
                        )

                        if column in self._primary_tprms and not value:
                            self._error_row_with_reasons[column].append(
                                BatchPreviewErrorInstance(
                                    error_value=formula_tprm.constraint,
                                    status=NOT_VALID_VALUE_TYPE,
                                    index_of_error_value=index,
                                )
                            )

                            not_valid_formula_indexes.append(index)

                        # set new calculated values
                        self._main_dataframe.at[index, column] = value

                    except ValueError:
                        self._error_row_with_reasons[column].append(
                            BatchPreviewErrorInstance(
                                error_value=formula_tprm.constraint,
                                status=NOT_VALID_VALUE_TYPE,
                                index_of_error_value=index,
                            )
                        )

                    if self._error_row_with_reasons[column]:
                        statuses_by_index = {
                            error.index_of_error_value: error.status
                            for error in self._error_row_with_reasons[column]
                        }
                        column_name = self._new_column_name_mapping[column]
                        raise ColumnValuesValidationError(
                            status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                            detail=f"There are error data in primary "
                            f"column {column_name}. "
                            f"Error statuses by index: {statuses_by_index}",
                        )

        # delete wrong primary formula rows
        if not_valid_formula_indexes:
            self._main_dataframe.drop(not_valid_formula_indexes, inplace=True)

        return self._main_dataframe

    def _change_df_if_inherit_location(self) -> pd.DataFrame:
        parent_attribute = "p_id"

        if "parent_name" in self._main_dataframe.columns:
            parent_attribute = "parent_name"

        parent_ids_to_search = MO.id.in_(
            map(int, self._main_dataframe[parent_attribute].dropna().unique())
        )
        stmt = select(MO).where(
            and_(
                parent_ids_to_search,
                MO.tmo_id == self._object_type_instance.p_id,
            )
        )
        parent_mos: list[MO] = self._session.execute(stmt).scalars().all()

        dict_parent_mo = {str(mo.id): mo for mo in parent_mos}

        aliased_columns = {
            "point_a_id": "point_a_id"
            if "point_a_name" in self._main_dataframe.columns
            else "point_a_name",
            "point_b_id": "point_b_id"
            if "point_b_name" in self._main_dataframe.columns
            else "point_b_name",
        }

        for idx, row in self._main_dataframe.iterrows():
            if pd.notna(row[parent_attribute]):
                location_data = extract_location_data(
                    geometry_type=self._object_type_instance.geometry_type,
                    parent_mo=dict_parent_mo[str(int(row[parent_attribute]))],
                )
                for column_name, row_value in location_data.items():
                    aliased_name = aliased_columns.get(column_name)
                    row_value = (
                        json.dumps(row_value)
                        if isinstance(row_value, dict)
                        else row_value
                    )

                    if aliased_name:
                        self._main_dataframe.loc[idx, aliased_name] = row_value
                        continue

                    self._main_dataframe.loc[idx, column_name] = row_value

        if "point_a_id" in self._main_dataframe.columns:
            self._main_dataframe["point_a_id"] = self._main_dataframe[
                "point_a_id"
            ].astype(int)

        if "point_b_id" in self._main_dataframe.columns:
            self._main_dataframe["point_b_id"] = self._main_dataframe[
                "point_b_id"
            ].astype(int)
        return self._main_dataframe

    def _process_inherit_location(self) -> DataFrame:
        # if mo inherited we need update geometry exclude case where we manually set point_a/b
        if (
            self._object_type_instance.inherit_location
            and (
                "p_id" in self._main_dataframe
                or "parent_name" in self._main_dataframe
            )
            and (
                "point_a_id" not in self._main_dataframe
                or "point_a_name" not in self._main_dataframe
            )
            and (
                "point_b_id" not in self._main_dataframe
                or "point_b_name" not in self._main_dataframe
            )
        ):
            self._main_dataframe = self._change_df_if_inherit_location()

        return self._main_dataframe

    def _get_replaced_single_mo_links_to_real_mo_names_for_primary(
        self, mo_link_ids: list[int]
    ) -> list[str]:
        """Used for primary TPRM - returns only MO names without TMO prefix"""
        mo_id_and_name = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(set(mo_link_ids)):
            res = self._session.exec(
                select(MO.id, MO.name).where(
                    MO.id.in_([int(mo_id) for mo_id in chunk])
                )
            ).all()
            mo_id_and_name.update({mo_id: mo_name for mo_id, mo_name in res})

        converted_values = []
        for mo_id in mo_link_ids:
            converted_values.append(mo_id_and_name.get(mo_id))

        return converted_values

    def _get_replaced_single_mo_links_to_real_mo_names(
        self, mo_link_ids: list[int]
    ) -> list[str]:
        """Used for export - returns format 'MO_name:TMO_name'"""
        mo_id_and_name = {}
        mo_tmo_mapping = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(set(mo_link_ids)):
            res = self._session.exec(
                select(MO.id, MO.name, MO.tmo_id).where(
                    MO.id.in_([int(mo_id) for mo_id in chunk])
                )
            ).all()
            mo_id_and_name.update(
                {mo_id: mo_name for mo_id, mo_name, tmo_id in res}
            )
            mo_tmo_mapping.update(
                {mo_id: tmo_id for mo_id, mo_name, tmo_id in res}
            )

        tmo_ids = set(mo_tmo_mapping.values())
        tmo_names_by_id = {}
        if tmo_ids:
            for chunk in get_chunked_values_by_sqlalchemy_limit(tmo_ids):
                stmt = select(TMO.id, TMO.name).where(TMO.id.in_(chunk))
                tmo_data = self._session.exec(stmt).all()
                tmo_names_by_id.update(
                    {tmo_id: tmo_name for tmo_id, tmo_name in tmo_data}
                )

        converted_values = []
        for mo_id in mo_link_ids:
            mo_name = mo_id_and_name[mo_id]
            tmo_id = mo_tmo_mapping[mo_id]
            tmo_name = tmo_names_by_id[tmo_id]
            converted_values.append(f"{mo_name}:{tmo_name}")

        return converted_values

    def _combine_object_names_from_file_data(self):
        """
        This method collect all primary TPRM ids and parent object name(if it in columns),
        and after that gather these data to object names to new column
        """
        primary_columns = [
            str(tprm_id) for tprm_id in self._object_type_instance.primary
        ]

        if not self._object_type_instance.global_uniqueness and primary_columns:
            if "p_id" in self._main_dataframe.columns:
                primary_columns.insert(0, "p_id")

            elif "parent_name" in self._main_dataframe.columns:
                primary_columns.insert(0, "parent_name")

        primary_mo_link_tprms = [
            str(tprm_id)
            for tprm_id in self._mo_link_tprms.keys()
            if str(tprm_id) in primary_columns
        ]

        dataframe_with_primary_values = self._main_dataframe[primary_columns]
        for primary_mo_link_tprm in primary_mo_link_tprms:
            mo_link_values = dataframe_with_primary_values[
                primary_mo_link_tprm
            ].values.tolist()
            mo_link_names = (
                self._get_replaced_single_mo_links_to_real_mo_names_for_primary(
                    mo_link_values
                )
            )
            dataframe_with_primary_values[primary_mo_link_tprm] = mo_link_names

        if "parent_name" in primary_columns:
            parent_ids = dataframe_with_primary_values[
                "parent_name"
            ].values.tolist()
            parent_names_by_id = self._parent_id_column_converter(parent_ids)
            dataframe_with_primary_values["parent_name"] = [
                parent_names_by_id.get(parent_id) for parent_id in parent_ids
            ]

        elif "p_id" in primary_columns:
            parent_ids = dataframe_with_primary_values["p_id"].values.tolist()
            parent_names_by_id = self._parent_id_column_converter(parent_ids)
            dataframe_with_primary_values["p_id"] = [
                parent_names_by_id.get(parent_id) for parent_id in parent_ids
            ]

        dataframe_with_primary_values_str = (
            dataframe_with_primary_values.astype(str)
        )
        dataframe_with_primary_values_str = (
            dataframe_with_primary_values_str.replace("None", "")
        )

        def safe_join_row(row):
            valid_values = [
                str(val) for val in row if pd.notna(val) and str(val) != ""
            ]
            result = NAME_DELIMITER.join(valid_values)
            return result

        self._main_dataframe[COMBINED_NAMES_COLUMN] = (
            dataframe_with_primary_values_str.apply(safe_join_row, axis=1)
        )

        self._combined_object_names_from_dataframe: Series = (
            self._main_dataframe[COMBINED_NAMES_COLUMN]
        )

        del dataframe_with_primary_values

    def _set_label_values(self):
        if self._object_type_instance.label:
            label_tprm_ids = [
                str(tprm_id) for tprm_id in self._object_type_instance.label
            ]
            dataframe_with_label_values = self._main_dataframe[label_tprm_ids]

            dataframe_with_label_values_str = (
                dataframe_with_label_values.astype(str).replace("None", "")
            )

            def safe_join_label_row(row):
                valid_values = [
                    str(val) for val in row if pd.notna(val) and str(val) != ""
                ]
                return NAME_DELIMITER.join(valid_values)

            self._main_dataframe["label"] = (
                dataframe_with_label_values_str.apply(
                    safe_join_label_row, axis=1
                )
            )
            del dataframe_with_label_values

    def _validate_and_replace_mo_duplicates(self) -> None:
        """
        Remove duplicates from input sheet by self._combined_object_names_from_dataframe
        and update our errors.
        """
        duplicate_groups = defaultdict(list)
        for index, name in self._combined_object_names_from_dataframe.items():
            if name:
                duplicate_groups[name].append(index)

        indexes_to_delete = list()

        for name, indexes in duplicate_groups.items():
            first_index = indexes[0]
            duplicate_indexes = indexes[1:]
            if duplicate_indexes:
                reason_message = get_reason_message(
                    DUPLICATED_OBJECT_NAMES, indexes
                )
                error_instance = BatchPreviewErrorInstance(
                    error_value=name,
                    index_of_error_value=first_index,
                    status=reason_message,
                )
                self._error_row_with_reasons["Object Name"].append(
                    error_instance
                )
                match self._raise_error_status:
                    case ErrorProcessor.RAISE.value:
                        raise DuplicatedMONameInFile(
                            status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                            detail=f"There are duplicated object name in file: {name}",
                        )

                indexes_to_delete.extend(duplicate_indexes)

        self._main_dataframe.drop(indexes_to_delete, inplace=True)
        self._combined_object_names_from_dataframe.drop(
            indexes_to_delete, inplace=True
        )

    def _get_exists_object_names(self):
        """
        This method finds real exists object names from file collected data
        """
        combined_object_names = (
            self._combined_object_names_from_dataframe.values.tolist()
        )

        self._exists_object_names_with_ids = self._get_object_names_by_ids(
            object_names=combined_object_names,
            object_type=self._object_type_instance,
        )

        self._exists_object_names = list(
            self._exists_object_names_with_ids.values()
        )

    def _separate_object_to_create_and_objects_for_delete(
        self,
    ) -> DataframesForCreateAndUpdate:
        """
        This method divides main dataframe into two dataframes:
            - dataframes, with objects, that need to created
            - dataframes, with objects, that need to update
        """

        # sort exists objects and objects, which need to be created
        object_exists_result = self._main_dataframe[COMBINED_NAMES_COLUMN].isin(
            self._exists_object_names
        )

        exists_objects_indexes = object_exists_result[
            object_exists_result == True  # noqa
        ].index.tolist()

        self._objects_to_update_dataframe = self._main_dataframe.loc[
            exists_objects_indexes
        ].reset_index(drop=True)

        self._create_object_parameters_and_attributes = (
            self._main_dataframe.drop(exists_objects_indexes).reset_index(
                drop=True
            )
        )

        return DataframesForCreateAndUpdate(
            dataframe_with_data_for_create=self._create_object_parameters_and_attributes,
            dataframe_with_data_for_update=self._objects_to_update_dataframe,
        )

    def _map_object_names_with_object_ids(self):
        """
        This method add to dataframe with objects, which need to be updated,
        MO_IDs by earlier gathered combined names
        """
        object_id_by_name = {
            mo_name: mo_id
            for mo_id, mo_name in self._exists_object_names_with_ids.items()
        }

        combined_names_column = self._objects_to_update_dataframe[
            COMBINED_NAMES_COLUMN
        ]

        self._objects_to_update_dataframe[MO_ID_COLUMN] = (
            combined_names_column.map(object_id_by_name)
        )

    def _process_prm_link_values_for_created_objects(self):
        """
        This method read objects, which already created and validate these parameters,
        reading parameters from dataframe, which are ready to be created
        """
        if self._objects_to_update_dataframe.any().any():
            if self._prm_link_tprms_with_prm_link_filter:
                errors = self._batch_prm_link_filter_validation_for_exists_objects(
                    dataframe_with_data=self._objects_to_update_dataframe,
                    prm_link_tprms_with_prm_link_filter=self._prm_link_tprms_with_prm_link_filter,
                    prm_link_tprms=self._prm_link_tprms,
                )

                self._error_row_with_reasons.update(errors)

            # convert prm link values
            for tprm_id, tprm_instance in self._prm_link_tprms.items():
                column_values = self._create_object_parameters_and_attributes[
                    str(tprm_id)
                ].values.tolist()

                converted_values = self._convert_tprm_values(
                    parameter_type_instance=tprm_instance,
                    values_to_convert=column_values,
                )

                self._create_object_parameters_and_attributes[str(tprm_id)] = (
                    converted_values
                )

    def _batch_prm_link_filter_validation_for_exists_objects(
        self,
        dataframe_with_data: DataFrame,
        prm_link_tprms: dict[int, TPRM],
        prm_link_tprms_with_prm_link_filter: dict[int, TPRM],
    ):
        errors: dict[str, list[BatchPreviewErrorInstance]] = defaultdict(list)

        mo_with_parameters = defaultdict(dict)
        mo_ids_for_update = (
            dataframe_with_data[MO_ID_COLUMN].unique().values.tolist()
        )

        # collect params from db (already exists params)
        for chunk in get_chunked_values_by_sqlalchemy_limit(mo_ids_for_update):
            stmt = select(PRM.mo_id, PRM.value, PRM.tprm_id).where(
                PRM.mo_id.in_(chunk)
            )
            temp_res = self._session.execute(stmt).scalars().all()
            for mo_id, value, tprm_id in temp_res:
                mo_with_parameters[mo_id].update({tprm_id: value})

        # collect params from requested file
        for _, row in dataframe_with_data.iterrows():
            data = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
            params = {}
            for tprm_id, value in data.items():
                if tprm_id.isdigit() and tprm_id not in prm_link_tprms:
                    params[int(tprm_id)] = value

            mo_with_parameters[row[MO_ID_COLUMN]].update(params)

        # format parameter to class
        for mo_id, tprm_with_values in mo_with_parameters.items():
            for tprm_id, value in tprm_with_values.items():
                mo_with_parameters[mo_id] = PRMCreateByMO(
                    tprm_id=tprm_id, value=value
                )  # noqa

        # collect internals and externals for every prm link tprm
        internal_external_for_tprms: dict[str, dict] = {}
        for tprm_id, tprm_ints in prm_link_tprms_with_prm_link_filter.items():
            regex = re.compile(r"(\d+):(\d+)")
            internal_tprm_id, external_tprm_id = regex.findall(
                tprm_ints.prm_link_filter
            )[0]
            internal_external_for_tprms[str(tprm_id)] = {
                "internal_tprm_id": int(internal_tprm_id),
                "external_tprm_id": int(external_tprm_id),
            }

        # main prm links validator module
        for _, row in dataframe_with_data.iterrows():
            for (
                tprm_id,
                tprm_inst,
            ) in prm_link_tprms_with_prm_link_filter.items():
                prm_link_ids = row[str(tprm_id)]
                mo_id_for_row = row[MO_ID_COLUMN]
                db_object_params = mo_with_parameters[mo_id_for_row]

                internal_tprm_id = internal_external_for_tprms[str(tprm_id)][
                    "internal_tprm_id"
                ]
                external_tprm_id = internal_external_for_tprms[str(tprm_id)][
                    "external_tprm_id"
                ]

                internal_parameter_link = None
                for prm in db_object_params:
                    if prm.tprm_id == internal_tprm_id:
                        internal_parameter_link = prm
                        break

                if internal_parameter_link:
                    possible_prm_ids = get_possible_prm_ids_for_internal_link(
                        session=self._session,
                        external_tprm_id=external_tprm_id,
                        internal_parameter_link=internal_parameter_link,
                        db_param_type=tprm_inst,
                    )
                else:
                    possible_prm_ids = get_possible_prm_ids_for_external_link(
                        session=self._session,
                        external_tprm_id=external_tprm_id,
                        db_param_type=tprm_inst,
                    )

                if tprm_inst.multiple:
                    for linked_parameter_id in prm_link_ids:
                        if linked_parameter_id not in possible_prm_ids:
                            errors[str(tprm_id)].append(
                                BatchPreviewErrorInstance(
                                    error_value=linked_parameter_id,
                                    status=NOT_VALID_BY_PRM_LINK_FILTER,
                                )
                            )
                else:
                    if prm_link_ids not in possible_prm_ids:
                        errors[str(tprm_id)].append(
                            BatchPreviewErrorInstance(
                                error_value=prm_link_ids,
                                status=NOT_VALID_BY_PRM_LINK_FILTER,
                            )
                        )
        return errors

    def _batch_prm_link_filter_validation_for_not_exists_objects(
        self,
        dataframe_with_data: DataFrame,
        prm_link_tprms_with_prm_link_filter: dict[int, TPRM],
    ):
        errors = {}

        # collect internals and externals for every prm link tprm
        internal_external_for_tprms: dict[str, dict] = {}
        for tprm_id, tprm_ints in prm_link_tprms_with_prm_link_filter.items():
            regex = re.compile(r"(\d+):(\d+)")
            internal_tprm_id, external_tprm_id = regex.findall(
                tprm_ints.prm_link_filter
            )[0]
            internal_external_for_tprms[str(tprm_id)] = {
                "internal_tprm_id": int(internal_tprm_id),
                "external_tprm_id": int(external_tprm_id),
            }

        # main prm links validator module
        for _, row in dataframe_with_data.iterrows():
            data = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
            params = [
                PRMCreateByMO(tprm_id=int(key), value=value)
                for key, value in data.items()
                if key.isdigit()
            ]

            for (
                tprm_id,
                tprm_inst,
            ) in prm_link_tprms_with_prm_link_filter.items():
                prm_link_ids = row[str(tprm_id)]

                internal_tprm_id = internal_external_for_tprms[str(tprm_id)][
                    "internal_tprm_id"
                ]
                external_tprm_id = internal_external_for_tprms[str(tprm_id)][
                    "external_tprm_id"
                ]

                internal_parameter_link = None
                for prm in params:
                    if prm.tprm_id == internal_tprm_id:
                        internal_parameter_link = prm
                        break

                if internal_parameter_link:
                    possible_prm_ids = get_possible_prm_ids_for_internal_link(
                        session=self._session,
                        external_tprm_id=external_tprm_id,
                        internal_parameter_link=internal_parameter_link,
                        db_param_type=tprm_inst,
                    )
                else:
                    possible_prm_ids = get_possible_prm_ids_for_external_link(
                        session=self._session,
                        external_tprm_id=external_tprm_id,
                        db_param_type=tprm_inst,
                    )

                if tprm_inst.multiple:
                    for linked_parameter_id in prm_link_ids:
                        if linked_parameter_id not in possible_prm_ids:
                            original_index = row[COLUMN_WITH_ORIGINAL_INDEXES]
                            errors[tprm_id][original_index] = (
                                NOT_VALID_BY_PRM_LINK_FILTER
                            )
                else:
                    if prm_link_ids not in possible_prm_ids:
                        original_index = row[COLUMN_WITH_ORIGINAL_INDEXES]
                        errors[tprm_id][original_index] = (
                            NOT_VALID_BY_PRM_LINK_FILTER
                        )

        return errors

    def _process_prm_link_values_for_not_created_objects(self):
        """
        This method read objects, which are requested as those, which have to be created
         and validate these parameters, reading parameters from dataframe, which are ready to be created
        """
        if self._create_object_parameters_and_attributes.any().any():
            # validate data
            if self._prm_link_tprms_with_prm_link_filter:
                errors = self._batch_prm_link_filter_validation_for_not_exists_objects(
                    dataframe_with_data=self._objects_to_update_dataframe,
                    prm_link_tprms_with_prm_link_filter=self._prm_link_tprms_with_prm_link_filter,
                )
                self._error_row_with_reasons.update(errors)

            if self._two_way_mo_link_tprms:
                errors = {}
                row_ids_to_drop = []

                two_way_mo_link_tprms = {
                    tprm.id: tprm for tprm in self._two_way_mo_link_tprms
                }

                for tprm_id, tprm in two_way_mo_link_tprms:
                    new_parameters = dict(
                        zip(
                            self._create_object_parameters_and_attributes[
                                COLUMN_WITH_ORIGINAL_INDEXES
                            ],
                            self._create_object_parameters_and_attributes[
                                str(tprm_id)
                            ],
                        )
                    )

                    # where mo_id = index of row
                    mos = {
                        mo_id: MO(tmo_id=self._object_type_instance.id)
                        for mo_id in new_parameters.keys()
                    }

                    mo_link_errors, _ = check_create_two_way_prms(
                        session=self._session,
                        new_prms=new_parameters,
                        tprms={tprm_id: tprm},
                        mos=mos,
                    )

                    if mo_link_errors:
                        wrong_indexes = {
                            error.prm.mo_id for error in mo_link_errors
                        }

                        indexes_to_drop = (
                            self._create_object_parameters_and_attributes[
                                ~self._create_object_parameters_and_attributes[
                                    COLUMN_WITH_ORIGINAL_INDEXES
                                ].isin(wrong_indexes)
                            ].index.tolist()
                        )

                        row_ids_to_drop.extend(indexes_to_drop)

                        for wrong_index in wrong_indexes:
                            errors[str(tprm_id)][wrong_index] = NOT_VALID_VALUE

                self._error_row_with_reasons.update(errors)
                self._create_object_parameters_and_attributes.drop(
                    row_ids_to_drop
                )

    @staticmethod
    def _get_dataframe_without_rows_where_indexes_in_column(
        dataframe: DataFrame,
        indexes_to_exclude: list[int],
    ) -> DataFrame:
        return dataframe.drop(index=indexes_to_exclude, errors="ignore")

    def _get_parameter_values_by_mo_id_for_specific_tprm(
        self, tprm_instance: TPRM, mo_ids: list[int]
    ) -> dict[int, str]:
        db_result_parameter_by_id = {}

        for chunk in get_chunked_values_by_sqlalchemy_limit(mo_ids):
            stmt = select(PRM.mo_id, PRM.value).where(
                PRM.tprm_id == tprm_instance.id, PRM.mo_id.in_(chunk)
            )
            exists_parameters = self._session.exec(stmt).all()

            if tprm_instance.multiple:
                db_result_parameter_by_id.update(
                    {
                        mo_id: pickle.loads(bytes.fromhex(value))
                        for mo_id, value in exists_parameters
                    }
                )

            else:
                db_result_parameter_by_id.update(
                    {mo_id: value for mo_id, value in exists_parameters}
                )

        return db_result_parameter_by_id

    def _get_mo_id_by_parameter_ids_for_specific_tprm(
        self, tprm_instance: TPRM, mo_ids: list[int]
    ) -> dict[int, int]:
        mo_id_with_prm_id = {}

        for chunk in get_chunked_values_by_sqlalchemy_limit(mo_ids):
            stmt = select(PRM.id, PRM.mo_id).where(
                PRM.tprm_id == tprm_instance.id, PRM.mo_id.in_(chunk)
            )
            exists_parameters = self._session.exec(stmt).all()

            mo_id_with_prm_id.update(
                {mo_id: prm_id for prm_id, mo_id in exists_parameters}
            )

        return mo_id_with_prm_id

    @staticmethod
    def _fill_dataframe_data(dataframe: DataFrame, **kwargs):
        for column, values in kwargs.items():
            dataframe[column] = values
        return dataframe

    def _fill_updated_prms_dataframe(
        self,
        updated_mo_prms_temp: DataFrame,
        values_of_column_with_mo_ids: DataFrame,
        column_name_with_new_values: str,
        old_parameter_values: dict[int, str],
        parameter_id_by_mo_id: dict[int, int],
    ) -> DataFrame:
        original_indexes = values_of_column_with_mo_ids[
            COLUMN_WITH_ORIGINAL_INDEXES
        ]
        mo_id_column = values_of_column_with_mo_ids[
            MO_ID_COLUMN
        ].values.tolist()
        new_value_column = values_of_column_with_mo_ids[
            column_name_with_new_values
        ].values.tolist()

        prm_id_column = values_of_column_with_mo_ids[MO_ID_COLUMN].map(
            parameter_id_by_mo_id
        )
        old_value_column = values_of_column_with_mo_ids[MO_ID_COLUMN].map(
            old_parameter_values
        )

        tprm_id_column = [
            column_name_with_new_values
            for _ in range(len(values_of_column_with_mo_ids))
        ]

        updated_mo_prms_temp = self._fill_dataframe_data(
            dataframe=updated_mo_prms_temp,
            old_value=old_value_column,
            new_value=new_value_column,
            tprm_id=tprm_id_column,
            prm_id=prm_id_column,
            mo_id=mo_id_column,
        )
        updated_mo_prms_temp[COLUMN_WITH_ORIGINAL_INDEXES] = original_indexes
        updated_mo_prms_temp = updated_mo_prms_temp.convert_dtypes()
        return updated_mo_prms_temp

    def _fill_created_parameters_dataframe(
        self,
        updated_mo_prms_temp: DataFrame,
        indexes_of_new_values: list[str],
        created_mo_prms_temp: DataFrame,
        values_of_column_with_mo_ids: DataFrame,
    ):
        mo_ids_column = updated_mo_prms_temp.loc[
            indexes_of_new_values, MO_ID_COLUMN
        ]
        value_column = updated_mo_prms_temp.loc[
            indexes_of_new_values, "new_value"
        ]
        tprm_id_column = updated_mo_prms_temp.loc[
            indexes_of_new_values, "tprm_id"
        ]

        created_mo_prms_temp = self._fill_dataframe_data(
            dataframe=created_mo_prms_temp,
            value=value_column,
            tprm_id=tprm_id_column,
            mo_id=mo_ids_column,
        )

        original_indexes = values_of_column_with_mo_ids[
            COLUMN_WITH_ORIGINAL_INDEXES
        ]
        created_mo_prms_temp[COLUMN_WITH_ORIGINAL_INDEXES] = original_indexes

        return created_mo_prms_temp

    def _fill_deleted_parameters_dataframe(
        self,
        updated_mo_prms_temp: DataFrame,
        deleted_object_values_temp: DataFrame,
        indexes_of_old_values: list[int],
        values_of_column_with_mo_ids: DataFrame,
    ):
        mo_ids_column = updated_mo_prms_temp.loc[
            indexes_of_old_values, MO_ID_COLUMN
        ]
        old_value_column = updated_mo_prms_temp.loc[
            indexes_of_old_values, "old_value"
        ]
        attr_column = updated_mo_prms_temp.loc[indexes_of_old_values, "tprm_id"]
        original_indexes = values_of_column_with_mo_ids[
            COLUMN_WITH_ORIGINAL_INDEXES
        ]

        deleted_object_values_temp = self._fill_dataframe_data(
            dataframe=deleted_object_values_temp,
            old_value=old_value_column,
            attr_name=attr_column,
            mo_id=mo_ids_column,
        )

        deleted_object_values_temp[COLUMN_WITH_ORIGINAL_INDEXES] = (
            original_indexes
        )
        return deleted_object_values_temp

    @staticmethod
    def _remove_rows_from_dataframe_where_values_in_two_columns_equals(
        dataframe: DataFrame,
        old_value_column_name: str,
        new_value_column_name: str,
    ):
        """
        This method filter requested dataframe from equal values in 2 columns, or if they are both Nan
        """
        dataframe = dataframe.replace({None: np.nan}).convert_dtypes()

        both_nan = (
            dataframe[new_value_column_name].isna()
            & dataframe[old_value_column_name].isna()
        )

        values_equal = dataframe[new_value_column_name].astype(
            str
        ) == dataframe[old_value_column_name].astype(str)

        # filter the dataframe by negating the combined condition
        updated_mo_attrs_temp = dataframe[~(both_nan | values_equal)]

        return updated_mo_attrs_temp

    def _get_created_updated_deleted_parameters_in_dataframes(
        self,
        updated_mo_prms_temp: DataFrame,
        created_mo_prms_temp: DataFrame,
        deleted_object_values_temp: DataFrame,
        values_of_column_with_mo_ids: DataFrame,
        tprm_instance: TPRM,
        mo_ids: list[int],
        column_name: str,
    ) -> ProcessedObjectParams:
        old_values: dict[int, str] = (
            self._get_parameter_values_by_mo_id_for_specific_tprm(
                tprm_instance=tprm_instance, mo_ids=mo_ids
            )
        )

        mo_id_with_prm_id = self._get_mo_id_by_parameter_ids_for_specific_tprm(
            tprm_instance=tprm_instance, mo_ids=mo_ids
        )

        updated_mo_prms_temp = self._fill_updated_prms_dataframe(
            updated_mo_prms_temp=updated_mo_prms_temp,
            values_of_column_with_mo_ids=values_of_column_with_mo_ids,
            column_name_with_new_values=column_name,
            old_parameter_values=old_values,
            parameter_id_by_mo_id=mo_id_with_prm_id,
        )

        # if old and new value are equals -- it means nothing to update/create/delete. we just delete
        updated_mo_prms_temp = (
            self._remove_rows_from_dataframe_where_values_in_two_columns_equals(
                dataframe=updated_mo_prms_temp,
                old_value_column_name="old_value",
                new_value_column_name="new_value",
            )
        )

        # if old value is None -- it was not created, so we need to create this one for already exists object
        empty_old_values = updated_mo_prms_temp[
            updated_mo_prms_temp["old_value"].isna()
        ]
        empty_old_values_indexes = empty_old_values.index.tolist()

        if empty_old_values_indexes:
            created_mo_prms_temp = self._fill_created_parameters_dataframe(
                updated_mo_prms_temp=updated_mo_prms_temp,
                indexes_of_new_values=empty_old_values_indexes,
                created_mo_prms_temp=created_mo_prms_temp,
                values_of_column_with_mo_ids=values_of_column_with_mo_ids,
            )

            updated_mo_prms_temp.drop(empty_old_values_indexes, inplace=True)

        # if new value is None -- it was written in user file, like that, which need to be deleted
        empty_new_values = updated_mo_prms_temp[
            updated_mo_prms_temp["new_value"].isna()
        ]
        empty_new_values_indexes = empty_new_values.index.tolist()

        if empty_new_values_indexes:
            deleted_object_values_temp = (
                self._fill_deleted_parameters_dataframe(
                    updated_mo_prms_temp=updated_mo_prms_temp,
                    deleted_object_values_temp=deleted_object_values_temp,
                    indexes_of_old_values=empty_new_values_indexes,
                    values_of_column_with_mo_ids=values_of_column_with_mo_ids,
                )
            )
            updated_mo_prms_temp.drop(empty_new_values_indexes, inplace=True)

        return ProcessedObjectParams(
            created_mo_prms_temp=created_mo_prms_temp,
            updated_mo_prms_temp=updated_mo_prms_temp,
            deleted_object_values_temp=deleted_object_values_temp,
        )

    @staticmethod
    def _combine_child_dataframe_to_main_dataframe(
        child_dataframe: DataFrame, main_dataframe: DataFrame
    ):
        main_dataframe = pd.concat(
            objs=[main_dataframe, child_dataframe], ignore_index=True
        )
        return main_dataframe

    def _get_db_parent_ids_by_mo_ids(
        self, object_type: TMO, all_mo_ids: list[list[int]]
    ) -> dict[int, str]:
        object_id_and_parent_id = {}

        for mo_ids in get_chunked_values_by_sqlalchemy_limit(all_mo_ids):
            exists_parameters = self._session.exec(
                select(MO.id, MO.p_id).where(MO.id.in_(mo_ids))
            ).all()

            object_id_and_parent_id.update(
                {mo_id: p_id for mo_id, p_id in exists_parameters}
            )

        return object_id_and_parent_id

    def _get_old_values_for_mo_attributes(
        self, mo_ids: list[int], mo_attribute: str
    ):
        mo_attributes = MO.__fields__.keys()
        old_values = {}
        if mo_attribute in mo_attributes:
            stmt = select(MO.id, getattr(MO, mo_attribute)).where(
                MO.id.in_(mo_ids)
            )
            exists_parameters = self._session.exec(stmt).all()

            old_values = {mo_id: value for mo_id, value in exists_parameters}

        return old_values

    def _get_pointed_object_name_by_requested_object_id(
        self,
        object_type: TMO,
        all_mo_ids: list[list[int]],
        point_values: list[str],
        point_type: Literal["point_a_id", "point_b_id"],
    ) -> dict[int, str]:
        db_point_values = {}

        point_ids = {}

        for chunk in get_chunked_values_by_sqlalchemy_limit(point_values):
            if object_type.points_constraint_by_tmo:
                stmt = select(MO.id, MO.name).where(
                    MO.id.in_(chunk),
                    MO.tmo_id.in_(object_type.points_constraint_by_tmo),
                )
            else:
                stmt = select(MO.id, MO.name).where(MO.id.in_(chunk))

            exists_points = self._session.exec(stmt).all()

            point_ids.update(
                {point_id: point_name for point_id, point_name in exists_points}
            )

        for mo_ids in all_mo_ids:
            stmt = select(MO.id, getattr(MO, point_type)).where(
                MO.id.in_(mo_ids)
            )
            points_for_objects = self._session.exec(stmt).all()

            db_point_values.update(
                {
                    mo_id: point_ids.get(point_id)
                    for mo_id, point_id in points_for_objects
                }
            )

        return db_point_values

    def _get_mo_attribute_values_by_attribute_name(
        self,
        mo_attribute_name: str,
        values_of_column_with_mo_ids: DataFrame,
    ) -> dict[int, str]:
        db_attributes_values = {}

        if mo_attribute_name not in {
            "parent_name",
            "point_a_name",
            "point_b_name",
        }:
            for chunk in range(
                0, len(values_of_column_with_mo_ids), SQLALCHEMY_LIMIT
            ):
                mo_ids_of_column = values_of_column_with_mo_ids[
                    MO_ID_COLUMN
                ].values.tolist()[chunk : chunk + SQLALCHEMY_LIMIT]

                db_attributes_values.update(
                    self._get_old_values_for_mo_attributes(
                        mo_ids=mo_ids_of_column, mo_attribute=mo_attribute_name
                    )
                )
        elif mo_attribute_name == "parent_name":
            db_attributes_values.update(
                self._get_db_parent_ids_by_mo_ids(
                    object_type=self._object_type_instance,
                    all_mo_ids=values_of_column_with_mo_ids[
                        MO_ID_COLUMN
                    ].values.tolist(),
                )
            )

        elif mo_attribute_name == "point_a_name":
            point_a_values = values_of_column_with_mo_ids[
                mo_attribute_name
            ].values.tolist()

            all_mo_ids = []
            for chunk in range(
                0, len(values_of_column_with_mo_ids), SQLALCHEMY_LIMIT
            ):
                all_mo_ids.append(
                    values_of_column_with_mo_ids[MO_ID_COLUMN][
                        chunk : chunk + SQLALCHEMY_LIMIT
                    ]
                )

            db_attributes_values.update(
                self._get_pointed_object_name_by_requested_object_id(
                    object_type=self._object_type_instance,
                    all_mo_ids=all_mo_ids,
                    point_values=point_a_values,
                    point_type="point_a_id",
                )
            )

        elif mo_attribute_name == "point_b_name":
            point_b_values = values_of_column_with_mo_ids[
                mo_attribute_name
            ].values.tolist()

            all_mo_ids = []
            for chunk in range(
                0, len(values_of_column_with_mo_ids), SQLALCHEMY_LIMIT
            ):
                all_mo_ids.append(
                    values_of_column_with_mo_ids[MO_ID_COLUMN][
                        chunk : chunk + SQLALCHEMY_LIMIT
                    ]
                )

            db_attributes_values.update(
                self._get_pointed_object_name_by_requested_object_id(
                    object_type=self._object_type_instance,
                    all_mo_ids=all_mo_ids,
                    point_values=point_b_values,
                    point_type="point_b_id",
                )
            )
        return db_attributes_values

    def _fill_updated_mo_attributes_dataframe(
        self,
        updated_mo_attrs_temp: DataFrame,
        mo_attribute_name: str,
        values_of_column_with_mo_ids: DataFrame,
    ):
        old_values = self._get_mo_attribute_values_by_attribute_name(
            mo_attribute_name=mo_attribute_name,
            values_of_column_with_mo_ids=values_of_column_with_mo_ids,
        )

        new_values_column = values_of_column_with_mo_ids[
            mo_attribute_name
        ].values.tolist()
        mo_ids_to_update = values_of_column_with_mo_ids[
            MO_ID_COLUMN
        ].values.tolist()
        attr_name_column = [
            mo_attribute_name for _ in range(len(new_values_column))
        ]
        old_values_column = [
            old_values[int(mo_id)] for mo_id in mo_ids_to_update
        ]

        updated_mo_attrs_temp = self._fill_dataframe_data(
            dataframe=updated_mo_attrs_temp,
            old_value=old_values_column,
            new_value=new_values_column,
            mo_id=mo_ids_to_update,
            attr_name=attr_name_column,
        )
        updated_mo_attrs_temp[COLUMN_WITH_ORIGINAL_INDEXES] = (
            values_of_column_with_mo_ids[COLUMN_WITH_ORIGINAL_INDEXES]
        )

        return updated_mo_attrs_temp

    def _fill_created_mo_attribute_dataframe(
        self,
        updated_mo_attrs_temp: DataFrame,
        created_mo_attrs_temp: DataFrame,
        indexes_with_new_values: list[int],
        values_of_column_with_mo_ids: DataFrame,
    ):
        mo_ids_column = updated_mo_attrs_temp.loc[
            indexes_with_new_values, MO_ID_COLUMN
        ]
        value_column = updated_mo_attrs_temp.loc[
            indexes_with_new_values, "new_value"
        ]
        attr_name_column = updated_mo_attrs_temp.loc[
            indexes_with_new_values, "attr_name"
        ]

        created_mo_attrs_temp = self._fill_dataframe_data(
            dataframe=created_mo_attrs_temp,
            mo_id=mo_ids_column,
            value=value_column,
            attr_name=attr_name_column,
        )
        created_mo_attrs_temp[COLUMN_WITH_ORIGINAL_INDEXES] = (
            values_of_column_with_mo_ids[COLUMN_WITH_ORIGINAL_INDEXES]
        )

        return created_mo_attrs_temp

    def _fill_deleted_mo_attributes(
        self,
        updated_mo_attrs_temp: DataFrame,
        deleted_object_values_temp: DataFrame,
        indexes_with_delete_values: DataFrame,
    ):
        mo_ids_column = updated_mo_attrs_temp.loc[
            indexes_with_delete_values, MO_ID_COLUMN
        ]
        old_value_column = updated_mo_attrs_temp.loc[
            indexes_with_delete_values, "old_value"
        ]
        attr_column = updated_mo_attrs_temp.loc[
            indexes_with_delete_values, "attr_name"
        ]

        deleted_object_values_temp = self._fill_dataframe_data(
            dataframe=deleted_object_values_temp,
            mo_id=mo_ids_column,
            old_value=old_value_column,
            attr_name=attr_column,
        )

        return deleted_object_values_temp

    def _get_column_error_indexes_of_column(self, column_name: str):
        return [
            error.index_of_error_value
            for error in self._error_row_with_reasons[column_name]
        ]

    def _get_created_updated_deleted_mo_attributes_in_dataframes(
        self,
        attribute_name: str,
        updated_mo_attrs_temp: DataFrame,
        created_mo_attrs_temp: DataFrame,
        deleted_object_values_temp: DataFrame,
        values_of_column_with_mo_ids: DataFrame,
    ) -> ProcessedObjectAttributes:
        updated_mo_attrs_temp = self._fill_updated_mo_attributes_dataframe(
            updated_mo_attrs_temp=updated_mo_attrs_temp,
            mo_attribute_name=attribute_name,
            values_of_column_with_mo_ids=values_of_column_with_mo_ids,
        )
        updated_mo_attrs_temp[COLUMN_WITH_ORIGINAL_INDEXES] = (
            values_of_column_with_mo_ids[COLUMN_WITH_ORIGINAL_INDEXES]
        )

        # remove equal "old value" and "new value". or where in old and in new values are None
        updated_mo_attrs_temp = (
            self._remove_rows_from_dataframe_where_values_in_two_columns_equals(
                dataframe=updated_mo_attrs_temp,
                old_value_column_name="old_value",
                new_value_column_name="new_value",
            )
        )

        # if old value is None - we create mo prm
        empty_old_values = updated_mo_attrs_temp[
            updated_mo_attrs_temp["old_value"].isna()
        ]
        empty_old_values_indexes = empty_old_values.index.tolist()

        if empty_old_values_indexes:
            created_mo_attrs_temp = self._fill_created_mo_attribute_dataframe(
                updated_mo_attrs_temp=updated_mo_attrs_temp,
                created_mo_attrs_temp=created_mo_attrs_temp,
                indexes_with_new_values=empty_old_values_indexes,
                values_of_column_with_mo_ids=values_of_column_with_mo_ids,
            )
            created_mo_attrs_temp[COLUMN_WITH_ORIGINAL_INDEXES] = (
                values_of_column_with_mo_ids[COLUMN_WITH_ORIGINAL_INDEXES]
            )
            updated_mo_attrs_temp.drop(empty_old_values_indexes, inplace=True)

        # if new value is None - its deleted prm`s
        empty_new_values = updated_mo_attrs_temp[
            updated_mo_attrs_temp["new_value"].isna()
        ]
        empty_new_values_indexes = empty_new_values.index.tolist()

        if empty_new_values_indexes:
            deleted_object_values_temp = self._fill_deleted_mo_attributes(
                updated_mo_attrs_temp=updated_mo_attrs_temp,
                deleted_object_values_temp=deleted_object_values_temp,
                indexes_with_delete_values=empty_new_values_indexes,
            )
            updated_mo_attrs_temp.drop(empty_new_values_indexes, inplace=True)

        return ProcessedObjectAttributes(
            created_mo_attrs_temp=created_mo_attrs_temp,
            updated_mo_attrs_temp=updated_mo_attrs_temp,
            deleted_object_values_temp=deleted_object_values_temp,
        )

    def _fill_result_dataframes_by_updated_values(self) -> ResultFiles:
        """
        This method creates dataframes, which contains data about parameters and MO attributes
        which will be CUD (created/updated/deleted)
        """
        if self._objects_to_update_dataframe.any().any():
            for column_name in self._objects_to_update_dataframe.columns:
                # primary columns can't be updated, but only created
                if (
                    column_name in SERVICE_COLUMNS
                    or column_name in self._primary_tprms
                ):
                    continue

                values_of_column_with_mo_ids = (
                    self._objects_to_update_dataframe[
                        [
                            MO_ID_COLUMN,
                            column_name,
                            COLUMN_WITH_ORIGINAL_INDEXES,
                        ]
                    ]
                )

                # CONSTANT's
                created_mo_attrs_temp = DataFrame(
                    columns=self._created_attributes.columns
                )
                updated_mo_attrs_temp = DataFrame(
                    columns=self._updated_object_attributes.columns
                )

                created_mo_prms_temp = DataFrame(
                    columns=self._created_mo_prms.columns
                )
                updated_mo_prms_temp = DataFrame(
                    columns=self._updated_parameters.columns
                )

                deleted_object_values_temp = DataFrame(
                    columns=self._deleted_object_values.columns
                )

                # filter dataframe to get result without error values
                error_indexes = self._get_column_error_indexes_of_column(
                    column_name
                )
                if error_indexes:
                    values_of_column_with_mo_ids = self._get_dataframe_without_rows_where_indexes_in_column(
                        dataframe=values_of_column_with_mo_ids,
                        indexes_to_exclude=error_indexes,
                    )

                # FILL CONSTANT DATAFRAMES
                if values_of_column_with_mo_ids.any().any():
                    mo_ids_of_column = (
                        values_of_column_with_mo_ids[MO_ID_COLUMN]
                        .unique()
                        .tolist()
                    )

                    if column_name.isdigit():
                        current_tprm_instance = self._tprm_instance_by_id[
                            int(column_name)
                        ]

                        processed_dataframe_values = self._get_created_updated_deleted_parameters_in_dataframes(
                            column_name=column_name,
                            created_mo_prms_temp=created_mo_prms_temp,
                            updated_mo_prms_temp=updated_mo_prms_temp,
                            deleted_object_values_temp=deleted_object_values_temp,
                            values_of_column_with_mo_ids=values_of_column_with_mo_ids,
                            tprm_instance=current_tprm_instance,
                            mo_ids=mo_ids_of_column,
                        )

                        if current_tprm_instance.val_type in [
                            "int",
                            "mo_link",
                            "prm_link",
                            two_way_mo_link_val_type_name,
                        ]:
                            created_mo_prms_temp = (
                                processed_dataframe_values.created_mo_prms_temp
                            )
                            updated_mo_prms_temp = (
                                processed_dataframe_values.updated_mo_prms_temp
                            )
                            deleted_object_values_temp = processed_dataframe_values.deleted_object_values_temp

                            processed_dataframe_values.created_mo_prms_temp = (
                                created_mo_prms_temp.astype(str)
                            )

                            processed_dataframe_values.updated_mo_prms_temp = (
                                updated_mo_prms_temp.astype(str)
                            )
                            deleted_params = deleted_object_values_temp.astype(
                                str
                            )
                            processed_dataframe_values.deleted_object_values_temp = deleted_params

                        self._created_mo_prms = self._combine_child_dataframe_to_main_dataframe(
                            child_dataframe=processed_dataframe_values.created_mo_prms_temp,
                            main_dataframe=self._created_mo_prms,
                        )

                        self._updated_parameters = self._combine_child_dataframe_to_main_dataframe(
                            child_dataframe=processed_dataframe_values.updated_mo_prms_temp,
                            main_dataframe=self._updated_parameters,
                        )

                        self._deleted_object_values = self._combine_child_dataframe_to_main_dataframe(
                            child_dataframe=processed_dataframe_values.deleted_object_values_temp,
                            main_dataframe=self._deleted_object_values,
                        )

                    else:
                        processed_dataframe_values = self._get_created_updated_deleted_mo_attributes_in_dataframes(
                            attribute_name=column_name,
                            updated_mo_attrs_temp=updated_mo_attrs_temp,
                            created_mo_attrs_temp=created_mo_attrs_temp,
                            deleted_object_values_temp=deleted_object_values_temp,
                            values_of_column_with_mo_ids=values_of_column_with_mo_ids,
                        )

                        self._created_attributes = self._combine_child_dataframe_to_main_dataframe(
                            child_dataframe=processed_dataframe_values.created_mo_attrs_temp,
                            main_dataframe=self._created_attributes,
                        )

                        self._updated_object_attributes = self._combine_child_dataframe_to_main_dataframe(
                            child_dataframe=processed_dataframe_values.updated_mo_attrs_temp,
                            main_dataframe=self._updated_object_attributes,
                        )

                        self._deleted_object_values = self._combine_child_dataframe_to_main_dataframe(
                            child_dataframe=processed_dataframe_values.deleted_object_values_temp,
                            main_dataframe=self._deleted_object_values,
                        )

        self._updated_dataframes = ResultFiles(
            self._updated_parameters,
            self._updated_object_attributes,
            self._created_attributes,
            self._deleted_object_values,
        )

        return self._updated_dataframes

    @staticmethod
    def _get_rows_for_specific_tprm_ids(
        dataframe: DataFrame, two_way_mo_link_tprm_ids: list[int]
    ):
        # remove all tprms, which are not tmo_way_mo_link
        dataframe_temp = dataframe[
            dataframe["tprm_id"].isin([two_way_mo_link_tprm_ids])
        ]

        # {tprm_id: {mo_id: [parameters_by_mo]}}
        parameters_for_two_way_mo_link = (
            dataframe_temp.groupby("tprm_id")
            .apply(
                lambda x: x.groupby("mo_id")["new_value"].apply(list).to_dict()
            )
            .to_dict()
        )

        return parameters_for_two_way_mo_link

    @staticmethod
    def _format_values_for_two_way_mo_link(
        parameters_for_two_way_mo_link: dict[int, dict[int, list]],
        class_to_format,
    ):
        for (
            tprm_id,
            mo_id_with_parameters,
        ) in parameters_for_two_way_mo_link.items():
            for mo_id, list_of_parameters in mo_id_with_parameters.items():
                formatted_params = [
                    class_to_format(value=value, tprm_id=tprm_id, mo_id=mo_id)
                    for value in list_of_parameters
                ]

                parameters_for_two_way_mo_link[tprm_id][mo_id] = (
                    formatted_params
                )

        return parameters_for_two_way_mo_link

    def _get_data_statistic(self) -> dict:
        # UPDATED MOs
        mo_ids_of_updated_mo_prms = (
            self._updated_parameters[MO_ID_COLUMN].unique().tolist()
        )
        mo_ids_of_updated_mo_attrs = (
            self._updated_object_attributes[MO_ID_COLUMN].unique().tolist()
        )
        mo_ids_of_created_mo_attrs = (
            self._created_attributes[MO_ID_COLUMN].unique().tolist()
        )
        mo_ids_of_deleted_params = (
            self._deleted_object_values[MO_ID_COLUMN].unique().tolist()
        )

        all_changed_mo_attrs = set(
            mo_ids_of_updated_mo_prms
            + mo_ids_of_updated_mo_attrs
            + mo_ids_of_deleted_params
            + mo_ids_of_created_mo_attrs
        )
        self._count_of_updated_mos = len(all_changed_mo_attrs)

        # CREATED PARAMETER's
        created_parameter_values = (
            self._create_object_parameters_and_attributes[self._tprm_ids]
            .replace("", None)
            .replace(np.nan, None)
        )
        quantity_of_created_prms = 0
        for column in self._tprm_ids:
            values = created_parameter_values[column].values.tolist()
            without_non_values = [
                v for v in values if v is not None and v != []
            ]
            len_of_column = len(without_non_values)
            quantity_of_created_prms += len_of_column

        self._count_of_created_prms = (
            len(self._created_mo_prms) + quantity_of_created_prms
        )

        # UPDATED PARAMETER's
        self._count_of_updated_mo_prms = len(self._updated_parameters) + len(
            self._updated_object_attributes
        )

        # DELETED PARAMETER's
        self._count_of_deleted_prms = len(self._deleted_object_values)

        # CREATED OBJECT's
        self._count_of_created_objects = len(
            self._create_object_parameters_and_attributes
        )

        return {
            "will_be_updated_mo": self._count_of_updated_mos,
            "will_be_created_mo": self._count_of_created_objects,
            "will_be_created_parameter_values": self._count_of_created_prms,
            "will_be_updated_parameter_values": self._count_of_updated_mo_prms,
            "will_be_deleted_parameter_values": self._count_of_deleted_prms,
        }

    def _validate_values_for_two_way_mo_link(
        self,
        created_mo_prms: DataFrame,
        updated_mo_prms: DataFrame,
        two_way_mo_link_tprms: list[TPRM],
    ):
        errors = defaultdict(list)
        warnings = []

        if two_way_mo_link_tprms:
            two_way_mo_link_tprms = {
                tprm.id: tprm for tprm in two_way_mo_link_tprms
            }
            two_way_mo_link_tprm_ids = list(two_way_mo_link_tprms.keys())

            if created_mo_prms.any().any():
                parameters_for_two_way_mo_link = (
                    self._get_rows_for_specific_tprm_ids(
                        dataframe=created_mo_prms,
                        two_way_mo_link_tprm_ids=two_way_mo_link_tprm_ids,
                    )
                )

                parameters_for_two_way_mo_link = self._format_values_for_two_way_mo_link(
                    parameters_for_two_way_mo_link=parameters_for_two_way_mo_link,
                    class_to_format=PRMCreateByMO,
                )

                mo_link_errors, _ = check_update_two_way_prms(
                    session=self._session,
                    update_prms=parameters_for_two_way_mo_link,
                    tprms_by_tprm_id=two_way_mo_link_tprms,
                )
                if mo_link_errors:
                    mo_ids_with_errors = {
                        error.prm.mo_id for error in mo_link_errors
                    }

                    wrongs_indexes = created_mo_prms[
                        created_mo_prms["mo_id"].isin(mo_ids_with_errors)
                    ][COLUMN_WITH_ORIGINAL_INDEXES]

                    rows_with_wrongs_indexes = created_mo_prms[
                        created_mo_prms[COLUMN_WITH_ORIGINAL_INDEXES].isin(
                            wrongs_indexes
                        )
                    ]

                    for _, row_values in rows_with_wrongs_indexes.iterrows():
                        errors[row_values["tprm_id"]][
                            row_values[COLUMN_WITH_ORIGINAL_INDEXES]
                        ] = NOT_VALID_VALUE

            if updated_mo_prms.any().any():
                parameters_for_two_way_mo_link = (
                    self._get_rows_for_specific_tprm_ids(
                        dataframe=created_mo_prms,
                        two_way_mo_link_tprm_ids=two_way_mo_link_tprm_ids,
                    )
                )

                parameters_for_two_way_mo_link = self._format_values_for_two_way_mo_link(
                    parameters_for_two_way_mo_link=parameters_for_two_way_mo_link,
                    class_to_format=PRMUpdateByMO,
                )

                mo_link_errors, _ = check_create_two_way_prms(
                    session=self._session,
                    new_prms=parameters_for_two_way_mo_link,
                    tprms=two_way_mo_link_tprms,
                )
                if mo_link_errors:
                    mo_ids_with_errors = {
                        error.prm.mo_id for error in mo_link_errors
                    }

                    wrongs_indexes = updated_mo_prms[
                        updated_mo_prms["mo_id"].isin(mo_ids_with_errors)
                    ][COLUMN_WITH_ORIGINAL_INDEXES]

                    rows_with_wrongs_indexes = updated_mo_prms[
                        updated_mo_prms[COLUMN_WITH_ORIGINAL_INDEXES].isin(
                            wrongs_indexes
                        )
                    ]

                    for _, row_values in rows_with_wrongs_indexes.iterrows():
                        errors[row_values["tprm_id"]][
                            row_values[COLUMN_WITH_ORIGINAL_INDEXES]
                        ] = NOT_VALID_VALUE

        return errors, warnings

    def _validate_two_way_mo_link(self):
        errors, _ = self._validate_values_for_two_way_mo_link(
            updated_mo_prms=self._updated_parameters,
            created_mo_prms=self._created_mo_prms,
            two_way_mo_link_tprms=self._two_way_mo_link_tprms,
        )

        self._error_row_with_reasons.update(errors)

    @staticmethod
    def _format_error_structure_for_batch_preview(
        error_values: list[Any], error_status: str, error_indexes: list[int]
    ):
        formatted_errors = []
        for error_value, error_index in zip(error_values, error_indexes):
            formatted_errors.append(
                BatchPreviewErrorInstance(
                    error_value=str(error_value),
                    index_of_error_value=error_index,
                    status=error_status,
                )
            )
        return formatted_errors

    @staticmethod
    def _format_warning_structure_for_batch_preview(
        warning_values: list[Any] | set[Any], warning_indexes: list[int]
    ):
        formatted_errors = []
        for index in range(len(warning_values)):
            formatted_errors.append(
                BatchPreviewWarningInstance(
                    warning_value=warning_values[index],
                    index_of_error_value=warning_indexes[index],
                )
            )
        return formatted_errors

    def _validate_datasets_if_sequences_exist_for_batch_preview(
        self,
        sequence_tprms: dict[int, TPRM],
        df_to_create_mo_and_prm: DataFrame,
        update_mo_prms: pd.DataFrame,
    ):
        errors = defaultdict(list)
        if not sequence_tprms:
            return errors

        # add absent sequences to dataframe
        for seq_tprm_id, seq_tprm in sequence_tprms.items():
            if seq_tprm.constraint and update_mo_prms.shape[0] > 0:
                tmp = update_mo_prms[
                    update_mo_prms["tprm_id"].isin(
                        [seq_tprm_id, int(seq_tprm.constraint)]
                    )
                ]
                tmp = tmp.duplicated(subset=["mo_id"], keep=False)
                if tmp.any():
                    errors[seq_tprm].extend(
                        self._format_error_structure_for_batch_preview(
                            error_values=tmp.values.tolist(),
                            error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                            error_indexes=tmp.index.tolist(),
                        )
                    )
            if str(seq_tprm_id) not in df_to_create_mo_and_prm.columns:
                df_to_create_mo_and_prm[str(seq_tprm_id)] = None

        # iterate by rows of creation dataset
        for idx, row in df_to_create_mo_and_prm.iterrows():
            # iterate by all sequences for each row
            for seq_tprm in sequence_tprms.values():
                if pd.isna(row[str(seq_tprm.id)]):
                    continue

                sequence_len = 0
                try:
                    # add count of db elements to sequence
                    query = select(func.count(PRM.id))
                    if seq_tprm.constraint:
                        sequence_type = str(row[seq_tprm.constraint])
                        query = query.where(
                            PRM.tprm_id == int(seq_tprm.constraint),
                            PRM.value == str(sequence_type),
                        )

                        df_sequence_count = df_to_create_mo_and_prm[
                            df_to_create_mo_and_prm[seq_tprm.constraint]
                            == sequence_type
                        ]

                        df_sequence_count = df_sequence_count.shape[0]
                    else:
                        query = query.where(PRM.tprm_id == seq_tprm.id)
                        df_sequence_count = df_to_create_mo_and_prm.shape[0]

                    db_sequence_count = self._session.execute(query)
                    sequence_len += db_sequence_count.scalar()
                    sequence_len += df_sequence_count

                    if int(row[str(seq_tprm.id)]) <= 0:
                        errors[str(seq_tprm.id)].append(
                            BatchPreviewErrorInstance(
                                error_value=row["value"],
                                status=SEQUENCE_LESS_THAN_0,
                                index_of_error_value=idx,
                            )
                        )

                    if int(row[str(seq_tprm.id)]) > sequence_len:
                        errors[str(seq_tprm.id)].append(
                            BatchPreviewErrorInstance(
                                error_value=row["value"],
                                status=SEQUENCE_LESS_THAN_SEQ_LENGTH,
                                index_of_error_value=idx,
                            )
                        )

                except HTTPException:
                    errors[str(seq_tprm.id)].append(
                        BatchPreviewErrorInstance(
                            error_value=row["value"],
                            status=NOT_VALID_VALUE_TYPE,
                            index_of_error_value=idx,
                        )
                    )

        for seq_tprm in sequence_tprms.values():
            update_df = update_mo_prms[update_mo_prms["tprm_id"] == seq_tprm.id]

            for idx, row in update_df.iterrows():
                sequence_len = 0
                try:
                    # add count of db elements to sequence
                    query = select(func.count(PRM.id))
                    if seq_tprm.constraint:
                        sequence_type_query = select(PRM.value).where(
                            PRM.mo_id == row["mo_id"],
                            PRM.tprm_id == int(seq_tprm.constraint),
                        )
                        sequence_type = self._session.execute(
                            sequence_type_query
                        )
                        sequence_type = sequence_type.scalar()
                        query = query.where(
                            PRM.tprm_id == int(seq_tprm.constraint),
                            PRM.value == sequence_type,
                        )
                        df_sequence_count = df_to_create_mo_and_prm[
                            df_to_create_mo_and_prm[seq_tprm.constraint]
                            == sequence_type
                        ]
                        df_sequence_count = df_sequence_count.shape[0]
                    else:
                        query = query.where(PRM.tprm_id == seq_tprm.id)
                        df_sequence_count = df_to_create_mo_and_prm.shape[0]

                    db_sequence_count = self._session.execute(query)
                    sequence_len += db_sequence_count.scalar()
                    sequence_len += df_sequence_count

                    if int(row["value"]) <= 0:
                        errors[str(seq_tprm.id)].append(
                            BatchPreviewErrorInstance(
                                error_value=row["value"],
                                status=SEQUENCE_LESS_THAN_0,
                                index_of_error_value=idx,
                            )
                        )

                    if int(row["value"]) > sequence_len:
                        errors[str(seq_tprm.id)].append(
                            BatchPreviewErrorInstance(
                                error_value=row["value"],
                                status=SEQUENCE_LESS_THAN_SEQ_LENGTH,
                                index_of_error_value=idx,
                            )
                        )

                except HTTPException:
                    errors[str(seq_tprm.id)].append(
                        BatchPreviewErrorInstance(
                            error_value=row["value"],
                            status=NOT_VALID_VALUE_TYPE,
                            index_of_error_value=idx,
                        )
                    )

        return errors

    def _validate_sequence(self):
        # sequence validation
        sequence_errors = self._validate_datasets_if_sequences_exist_for_batch_preview(
            df_to_create_mo_and_prm=self._create_object_parameters_and_attributes,
            update_mo_prms=self._updated_parameters,
            sequence_tprms=self._sequence_tprms,
        )

        # clean error lines, after sequence validation
        for column_name, error_instances in sequence_errors.items():
            match self._raise_error_status:
                case ErrorProcessor.RAISE.value:
                    statuses_by_index = {
                        error.index_of_error_value: error.status
                        for error in error_instances
                    }
                    column_name = self._new_column_name_mapping[column_name]
                    raise ColumnValuesValidationError(
                        status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                        detail=f"There are error data in primary column {column_name}. "
                        f"Error statuses by index: {statuses_by_index}",
                    )

            error_rows_ids = [
                error.index_of_error_value for error in error_instances
            ]
            if error_rows_ids:
                mask = self._updated_parameters["new_value"].isin(
                    error_rows_ids
                ) & (self._updated_parameters["tprm_id"] == column_name)
                self._updated_parameters = self._updated_parameters[~mask]

        self._error_row_with_reasons.update(sequence_errors)

    @staticmethod
    def _replace_values_in_dataframe_by_default_value(
        dataframe: DataFrame,
        default_value: Any,
        columns_with_replaces_values: dict[str, list[Any]],
    ) -> DataFrame:
        for (
            column_name,
            values_to_replace,
        ) in columns_with_replaces_values.items():
            if column_name != "Object Name":
                dataframe[column_name] = dataframe[column_name].apply(
                    lambda x: default_value if x in values_to_replace else x
                )

        return dataframe

    def _validate_headers_for_file(self, columns: list[str]):
        # Check if there are required MO fields in the file
        mo_required_fields = [
            k
            for k, v in MOBase.__fields__.items()
            if v.default is not None and k != "tmo_id"
        ]

        if mo_required_fields:
            mo_required_fields = set(mo_required_fields)
            if not mo_required_fields.issubset(columns):
                raise NotAddedRequiredAttributes(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                    detail=f"There are no all required Object(MO) fields in file. Missing "
                    f"Object(MO) fields "
                    f":{mo_required_fields.difference(columns)}",
                )

        requested_mo_attributes = {
            column for column in columns if not column.isdigit()
        }
        difference = requested_mo_attributes.difference(
            self._mo_attributes_available_for_batch
        )
        if difference:
            raise NotExistsMOAttributes(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                detail=f"There are names in the columns header that do not exist as MO"
                f" attributes. Do not exist: {difference}",
            )

    def _update_mo_attributes_by_service_tprms(self):
        """
        This method updates mo attributes values (latitude, longitude and status)
        by linked tprm ids in tmo. so if value by tprm, which can be latitude, lonhitude and status
        updates -- mo attribute has to ba updated
        """
        if self._latitude_tprm in self._main_dataframe.columns:
            self._main_dataframe["latitude"] = self._main_dataframe[
                self._latitude_tprm
            ]

        if self._longitude_tprm in self._main_dataframe.columns:
            self._main_dataframe["longitude"] = self._main_dataframe[
                self._longitude_tprm
            ]

        if self._status_tprm in self._main_dataframe.columns:
            self._main_dataframe["status"] = self._main_dataframe[
                self._status_tprm
            ]

    def _set_default_geometry_values(self):
        df = self._main_dataframe

        if "geometry" in df.columns:
            return

        points_in_columns = (
            "point_a_id" in df.columns and "point_b_id" in df.columns
        ) or ("point_a_name" in df.columns and "point_b_name" in df.columns)

        if (
            points_in_columns
            and self._object_type_instance.geometry_type != "line"
        ):
            df.loc[:, "geometry"] = None
            return

        for _, row_with_values in self._main_dataframe.iterrows():
            mo = MO(**row_with_values, tmo_id=self._object_type_instance.id)

            if points_in_columns:
                if "point_a_id" or "point_a_name" in row_with_values:
                    if row_with_values.get("point_a_id"):
                        stmt_point_a = select(MO).where(
                            MO.id == row_with_values["point_a_id"]
                        )
                    elif row_with_values.get("point_a_name"):
                        stmt_point_a = select(MO).where(
                            MO.id == row_with_values["point_a_name"]
                        )
                    point_a: MO = self._session.exec(stmt_point_a).first()

                if "point_b_id" or "point_b_name" in row_with_values:
                    if row_with_values.get("point_b_id"):
                        stmt_point_b = select(MO).where(
                            MO.id == row_with_values["point_b_id"]
                        )
                    elif row_with_values.get("point_b_name"):
                        stmt_point_b = select(MO).where(
                            MO.id == row_with_values["point_b_name"]
                        )

                    point_b: MO = self._session.exec(stmt_point_b).first()

                temp_geometry = update_geometry(
                    object_instance=mo, point_a=point_a, point_b=point_b
                )
                row_with_values["geometry"] = temp_geometry

    def _main_file_data_processing(
        self,
    ) -> tuple[ResultDataframes, ErrorInstances]:
        """
        This method operates:
         - all deep validation methods,
         - conversion row data from string type, as pandas represent to expected by val type
         - dataframes creation with parameters and MO attributes, which have to be created/updated/deleted
         - collect warnings and errors for rows
        """
        self._validate_headers_for_file(columns=self._main_dataframe.columns)

        self._main_dataframe[COLUMN_WITH_ORIGINAL_INDEXES] = (
            self._main_dataframe.index.tolist()
        )

        self._validate_file_values()

        error_values_by_column = defaultdict(list)
        for column_name, errors in self._error_row_with_reasons.items():
            for error in errors:
                error_values_by_column[column_name].append(error.error_value)

        # replace error values by None
        self._replace_values_in_dataframe_by_default_value(
            dataframe=self._main_dataframe,
            default_value=None,
            columns_with_replaces_values=error_values_by_column,
        )

        self._set_label_values()

        self._convert_dataframe_values_to_db_view()

        # we can process formula only after conversion rows data,
        # to get real val types, not string, as pandas present
        self._process_formula_data()

        self._process_inherit_location()

        self._set_default_geometry_values()

        self._main_dataframe = self._replace_values_in_dataframe(
            dataframe=self._main_dataframe, old_value=np.nan, new_value=None
        )

        self._update_mo_attributes_by_service_tprms()

        self._combine_object_names_from_file_data()

        self._validate_and_replace_mo_duplicates()

        self._get_exists_object_names()

        self._separate_object_to_create_and_objects_for_delete()

        self._map_object_names_with_object_ids()

        # prm links processes after all, because in dataframe can be parameters, on which
        # user counted on
        self._process_prm_link_values_for_created_objects()
        self._process_prm_link_values_for_not_created_objects()

        self._replace_values_in_dataframe(
            dataframe=self._main_dataframe, old_value=np.nan, new_value=None
        )

        self._fill_result_dataframes_by_updated_values()

        self._validate_two_way_mo_link()

        self._validate_sequence()

        self._result_dataframes = ResultDataframes(
            updated_mo_prms=self._updated_parameters,
            updated_mo_attrs=self._updated_dataframes.updated_mo_attrs,
            created_mo_prms=self._created_mo_prms,
            created_mo_attrs=self._updated_dataframes.created_mo_attrs,
            create_mo_and_prm_and_attr=self._create_object_parameters_and_attributes,
            deleted_object_values=self._updated_dataframes.deleted_object_values,
        )

        self._error_instances = ErrorInstances(
            warnings=self._warning_indexes,
            errors=self._error_row_with_reasons,
        )

        return self._result_dataframes, self._error_instances

    def _check_required_columns_in_file(self):
        stmt = select(cast(TPRM.id, String)).where(
            TPRM.tmo_id == self._object_type_instance.id,
            TPRM.required.is_(True),
        )
        required_tprm_ids = set(self._session.execute(stmt).scalars().all())
        used_rqeuired_tprm_in_file = required_tprm_ids.intersection(
            set(self._main_dataframe.columns)
        )

        not_used_required_tpms = [
            tprm_id
            for tprm_id in required_tprm_ids
            if tprm_id not in used_rqeuired_tprm_in_file
        ]

        if not_used_required_tpms:
            not_added_required_headers = (
                self._session.execute(
                    select(TPRM.name).where(TPRM.id.in_(not_used_required_tpms))
                )
                .scalars()
                .all()
            )

            raise NotAddedRequiredAttributes(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                detail=f"There are required TPRMs, "
                f"that were not added: {not_added_required_headers}",
            )

    def _drop_columns_which_requested_to_drop(self):
        for column in self._columns_to_drop:
            if column in self._main_dataframe:
                self._main_dataframe.drop(column, axis=1, inplace=True)

    def _drop_parent_columns_if_can_not_be_processed(self):
        parent_columns = {"p_id", "parent_name"}
        parent_columns_in_header = parent_columns.intersection(
            self._main_dataframe.columns
        )
        for parent_column_in_header in parent_columns_in_header:
            if self._object_type_instance.p_id is None:
                self._main_dataframe.drop(
                    parent_column_in_header, axis=1, inplace=True
                )

    def _prepare_data_for_process(self):
        """
        This method initialise collecting service data: from db, data from file,
        which will be needed in future
        """
        self._get_dataframe_from_file_data()
        self._drop_columns_which_requested_to_drop()
        self._drop_parent_columns_if_can_not_be_processed()
        self._validate_file_headers_for_uniqueness()

        self._update_header_of_dataframe_by_tprm_ids()

        self._tprm_ids: list[str] = [
            column_name
            for column_name in self._main_dataframe.columns
            if column_name.isdigit()
        ]

        self._get_tprm_instances_by_id(tprm_ids=self._tprm_ids)

        self._get_reversed_column_name_mapping()

        self._check_required_columns_in_file()

        # collecting data from file
        self._get_tprm_instances_by_val_types()
        self._get_primary_data_from_headers()
        self._get_mo_attrs_from_headers()

        # update service data
        self._main_dataframe = self._replace_values_in_dataframe(
            dataframe=self._main_dataframe, old_value=np.nan, new_value=None
        )

    def _validate_content_type_of_file_for_batch_mo_import(self):
        """
        Raises error if file has wrong content type
        """
        allowed_content_types = {
            "text/csv",
            "application/csv",
            "application/x-csv",
            "text/csv",
            "text/plain",
            "text/comma-separated-values",
            "text/x-comma-separated-values",
            "text/tab-separated-values",
            "application/vnd.ms-excel",
            "text/x-csv",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

        if self._file_content_type not in allowed_content_types:
            raise NotAllowedFileType(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY.value,
                detail=f"Content type: {self._file_content_type} is not allowed. Allowed "
                f"content types: {allowed_content_types}",
            )

    # MAIN METHOD's
    def _check(self):
        """
        This method just validate common cases, which are not connected to direct file`s data
        """
        self._validate_object_type(object_type=self._object_type_instance)
        self._validate_content_type_of_file_for_batch_mo_import()

    def validate_file_data(self) -> BatchImportValidatorResponse:
        self._check()

        # collecting data, easy validation
        self._prepare_data_for_process()

        # deep validation
        self._main_file_data_processing()

        return BatchImportValidatorResponse(
            result_dataframes=self._result_dataframes,
            error_instances=self._error_instances,
        )


class BatchImportPreview(BatchImportValidator):
    def __init__(
        self,
        file: bytes,
        session: Session,
        object_type_id: int,
        column_name_mapping: dict,
        delimiter: str,
        file_content_type: str,
    ):
        super().__init__(
            file=file,
            session=session,
            object_type_id=object_type_id,
            column_name_mapping=column_name_mapping,
            delimiter=delimiter,
            raise_errors=ErrorProcessor.COLLECT,
            file_content_type=file_content_type,
        )
        self._output = io.BytesIO()
        self._workbook = xlsxwriter.Workbook(self._output)

        self._warning_pattern_color = {"bg_color": "yellow"}

        # SHEET NAMES
        self._SUMMARY_SHEET_NAME: str = "Summary"
        self._DATA_MODEL_SHEET_NAME: str = "Data Model"
        self._NEW_OBJECTS_SHEET_NAME: str = "New"
        self._UPDATE_OBJECTS_SHEET_NAME: str = "Update"
        self._ERRORS_SHEET_NAME: str = "Errors"

    @staticmethod
    def _replace_value_in_dataframe(
        dataframe: DataFrame, old_value: Any, new_value: Any
    ) -> DataFrame:
        dataframe.replace({old_value: new_value}, inplace=True)
        return dataframe

    def _get_object_names_by_index(self) -> dict[int, str]:
        self._object_name_by_index_id = self._main_dataframe.set_index(
            COLUMN_WITH_ORIGINAL_INDEXES
        )[COMBINED_NAMES_COLUMN].to_dict()

        return self._object_name_by_index_id

    def _format_sheets_for_xlsx_file(self):
        data = {
            "Updated objects": self._count_of_updated_mos,
            "Created objects": self._count_of_created_objects,
            "Updated parameters": self._count_of_updated_mo_prms,
            "Created parameters": self._count_of_created_prms,
            "Deleted parameters": self._count_of_deleted_prms,
        }
        summary_report = SummaryReport(
            sheet_name=self._SUMMARY_SHEET_NAME,
            workbook=self._workbook,
            data_statistic=data,
        )
        data_model_report = DataModelReport(
            session=self._session,
            sheet_name=self._DATA_MODEL_SHEET_NAME,
            workbook=self._workbook,
            dominant_types=self._dominant_types,
            new_column_name_mapping=self._new_column_name_mapping,
            error_instances=self._error_instances,
            tprm_instance_by_id=self._tprm_instance_by_id,
        )
        error_sheet_report = ErrorSheetReport(
            sheet_name=self._ERRORS_SHEET_NAME,
            workbook=self._workbook,
            new_column_name_mapping=self._new_column_name_mapping,
            error_instances=self._error_instances,
            object_name_by_id=self._object_name_by_index_id,
            parent_name_by_id=self._parent_name_by_index_id,
            primary_tprms=self._primary_tprms,
        )
        create_sheet_report = CreateSheetReport(
            sheet_name=self._NEW_OBJECTS_SHEET_NAME,
            workbook=self._workbook,
            new_column_name_mapping=self._new_column_name_mapping,
            object_name_by_id=self._object_name_by_index_id,
            parent_name_by_id=self._parent_name_by_index_id,
            error_instances=self._error_instances,
            create_mo_and_prm_and_attr=self._create_object_parameters_and_attributes,
        )
        update_sheet_report = UpdateSheetReport(
            sheet_name=self._UPDATE_OBJECTS_SHEET_NAME,
            workbook=self._workbook,
            new_column_name_mapping=self._new_column_name_mapping,
            object_name_by_id=self._object_name_by_index_id,
            parent_name_by_id=self._parent_name_by_index_id,
            tprm_instance_by_id=self._tprm_instance_by_id,
            primary_tprms=self._primary_tprms,
            mo_data={
                "updated_mo_attrs": self._updated_object_attributes,
                "updated_mo_prms": self._updated_parameters,
                "created_mo_attrs": self._created_attributes,
                "created_mo_prms": self._created_mo_prms,
            },
        )

        summary_report.generate()
        data_model_report.generate()

        update_sheet_report.generate()
        create_sheet_report.generate()
        error_sheet_report.generate()

    def _get_error_values_by_column_names(self):
        self._all_error_values_by_column: dict[str, set[Any]] = dict()

        for col_name, error_instances in self._error_instances.errors.items():
            error_values = set()
            for error_instance in error_instances:
                error_values.add(error_instance.error_value)

            self._all_error_values_by_column[col_name] = error_values

    def _replace_prm_link_id_to_value(
        self, prm_link_ids: set[int]
    ) -> dict[int, Any]:
        mo_linked_prms = {}
        prm_link_and_value = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(prm_link_ids):
            stmt = (
                select(PRM.id, PRM.value, TPRM.val_type, TPRM.multiple)
                .join(TPRM)
                .where(PRM.id.in_(chunk))
            )
            query = self._session.exec(stmt).all()

            for prm_id, prm_value, val_type, multiple in query:
                if multiple:
                    prm_value = pickle.loads(bytes.fromhex(prm_value))

                if val_type == "mo_link":
                    mo_linked_prms[prm_id] = prm_value
                    continue

                prm_link_and_value[prm_id] = prm_value

        all_mo_links = []
        if mo_linked_prms:
            for mo_links in mo_linked_prms.values():
                if isinstance(mo_links, list):
                    all_mo_links.extend(mo_links)
                    continue
                all_mo_links.append(mo_links)

            mo_id_and_name = {}
            for chunk in all_mo_links:
                mo_links = self._session.exec(
                    select(MO.id, MO.name).where(MO.id.in_(chunk))
                ).all()
                mo_id_and_name.update(
                    {mo_id: mo_name for mo_id, mo_name in mo_links}
                )

            for prm_id, value in mo_linked_prms:
                if isinstance(value, list):
                    new_value = [mo_id_and_name[mo_link] for mo_link in value]
                else:
                    new_value = mo_id_and_name[value]

                prm_link_and_value[prm_id] = new_value

        return prm_link_and_value

    def _get_mo_names_by_mo_ids(self, mo_ids: set[int]) -> dict[int, str]:
        mo_id_and_name = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(mo_ids):
            res = self._session.exec(
                select(MO.id, MO.name).where(MO.id.in_(chunk))
            ).all()
            mo_id_and_name.update({mo_id: mo_name for mo_id, mo_name in res})
        return mo_id_and_name

    @staticmethod
    def _extract_multiple_and_single_values(
        column_values: Series,
    ) -> set[int | None]:
        column_values = column_values.replace({float("nan"): None})
        extracted_values = set()

        for value in column_values:
            if value is None:
                extracted_values.add(None)
                continue

            if isinstance(value, list):
                extracted_values.update(
                    {int(float(v)) for v in value if v is not None}
                )
            else:
                extracted_values.add(int(float(value)))

        return extracted_values

    @staticmethod
    def _extract_single_values(column_values: Series) -> set[int]:
        return set(value for value in column_values if isinstance(value, int))

    @staticmethod
    def _replace_value_by_mappings(
        mapping_values: dict[Any, Any],
        dataframe: DataFrame,
        attribute_column_name: str,
        value_column_name: str,
        attribute_name: str,
    ):
        for _, row in dataframe:
            if row[attribute_column_name] == attribute_name:
                row[value_column_name] = mapping_values.get(
                    row[value_column_name], row[value_column_name]
                )

        return dataframe

    @staticmethod
    def _old_replace_value_by_mappings(key, mapping_values: dict[int, str]):
        if key is None or pd.isna(key):
            return None

        if isinstance(key, list):
            return [mapping_values.get(v, v) for v in key]

        elif isinstance(key, int) or isinstance(key, float):
            return mapping_values.get(int(key), key)
        else:
            try:
                key = int(key)
                return mapping_values.get(int(key), key)
            except ValueError:
                return key

    def _convert_mo_links_ids_to_mo_names_in_dataframes(self):
        if self._mo_link_tprms:
            mo_link_columns = [
                str(tprm_id) for tprm_id in self._mo_link_tprms.keys()
            ]

            if self._updated_parameters.any().any():
                mo_link_rows = self._updated_parameters[
                    self._updated_parameters["tprm_id"].isin(mo_link_columns)
                ]

                if mo_link_rows.any().any():
                    old_values_set = self._extract_multiple_and_single_values(
                        mo_link_rows["old_value"].convert_dtypes()
                    )
                    new_values_set = self._extract_multiple_and_single_values(
                        mo_link_rows["new_value"].convert_dtypes()
                    )

                    all_mo_link_ids = old_values_set | new_values_set

                    mo_link_name_by_id: dict[int, str] = (
                        self._get_mo_names_by_mo_ids(mo_ids=all_mo_link_ids)
                    )

                    self._updated_parameters["tprm_id"] = (
                        self._updated_parameters["tprm_id"].astype(str)
                    )

                    self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            mo_link_columns
                        ),
                        "old_value",
                    ] = self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            mo_link_columns
                        ),
                        "old_value",
                    ].apply(lambda x: mo_link_name_by_id.get(int(float(x)), x))

                    self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            mo_link_columns
                        ),
                        "new_value",
                    ] = self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            mo_link_columns
                        ),
                        "new_value",
                    ].apply(lambda x: mo_link_name_by_id.get(int(float(x)), x))

                    del mo_link_name_by_id

            if self._created_mo_prms.any().any():
                mo_link_rows = self._created_mo_prms[
                    self._created_mo_prms["tprm_id"].isin(mo_link_columns)
                ]

                if mo_link_rows.any().any():
                    new_values_set = self._extract_multiple_and_single_values(
                        mo_link_rows["value"].convert_dtypes()
                    )

                    mo_link_name_by_id: dict[int, str] = (
                        self._get_mo_names_by_mo_ids(mo_ids=new_values_set)
                    )

                    self._created_mo_prms.loc[
                        self._created_mo_prms["tprm_id"].isin(mo_link_columns),
                        "value",
                    ] = self._created_mo_prms.loc[
                        self._created_mo_prms["tprm_id"].isin(mo_link_columns),
                        "value",
                    ].apply(lambda x: mo_link_name_by_id.get(int(float(x)), x))

                    del mo_link_name_by_id

            if self._create_object_parameters_and_attributes.any().any():
                columns_with_mo_link_values = (
                    self._create_object_parameters_and_attributes[
                        mo_link_columns
                    ]
                )

                if columns_with_mo_link_values.any().any():
                    all_mo_link_ids = set()

                    for column_name in mo_link_columns:
                        mo_link_ids = self._extract_multiple_and_single_values(
                            columns_with_mo_link_values[
                                column_name
                            ].convert_dtypes()
                        )
                        all_mo_link_ids.update(mo_link_ids)

                    mo_link_name_by_id: dict[int, str] = (
                        self._get_mo_names_by_mo_ids(mo_ids=all_mo_link_ids)
                    )

                    for column_name in mo_link_columns:
                        self._create_object_parameters_and_attributes[
                            column_name
                        ] = self._create_object_parameters_and_attributes[
                            column_name
                        ].apply(
                            self._old_replace_value_by_mappings,
                            mapping_values=mo_link_name_by_id,
                        )
                    del mo_link_name_by_id

            if self._deleted_object_values.any().any():
                mo_link_rows = self._deleted_object_values[
                    self._deleted_object_values["attr_name"].isin(
                        mo_link_columns
                    )
                ]

                if mo_link_rows.any().any():
                    new_values_set = self._extract_multiple_and_single_values(
                        mo_link_rows["old_value"].convert_dtypes()
                    )

                    mo_link_name_by_id: dict[int, str] = (
                        self._get_mo_names_by_mo_ids(mo_ids=new_values_set)
                    )

                    self._deleted_object_values.loc[
                        self._deleted_object_values["attr_name"].isin(
                            mo_link_columns
                        ),
                        "old_value",
                    ] = self._deleted_object_values.loc[
                        self._deleted_object_values["attr_name"].isin(
                            mo_link_columns
                        ),
                        "old_value",
                    ].apply(lambda x: mo_link_name_by_id.get(int(float(x)), x))

                    del mo_link_name_by_id

    def _convert_prm_links_ids_to_values_in_dataframes(self):
        if self._prm_link_tprms:
            prm_link_columns = [
                str(tprm_id) for tprm_id in self._prm_link_tprms.keys()
            ]

            if self._updated_parameters.any().any():
                prm_link_rows = self._updated_parameters[
                    self._updated_parameters["tprm_id"].isin(prm_link_columns)
                ]

                if prm_link_rows.any().any():
                    old_values_set = self._extract_multiple_and_single_values(
                        prm_link_rows["old_value"].convert_dtypes()
                    )
                    new_values_set = self._extract_multiple_and_single_values(
                        prm_link_rows["new_value"].convert_dtypes()
                    )

                    all_prm_link_ids = old_values_set | new_values_set

                    prm_link_value_by_id: dict[int, Any] = (
                        self._replace_prm_link_id_to_value(
                            prm_link_ids=all_prm_link_ids
                        )
                    )

                    self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            prm_link_columns
                        ),
                        "old_value",
                    ] = self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            prm_link_columns
                        ),
                        "old_value",
                    ].apply(lambda x: prm_link_value_by_id.get(int(x), x))

                    self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            prm_link_columns
                        ),
                        "new_value",
                    ] = self._updated_parameters.loc[
                        self._updated_parameters["tprm_id"].isin(
                            prm_link_columns
                        ),
                        "new_value",
                    ].apply(lambda x: prm_link_value_by_id.get(int(x), x))

                    del prm_link_value_by_id

            if self._created_mo_prms.any().any():
                prm_link_rows = self._created_mo_prms[
                    self._created_mo_prms["tprm_id"].isin(prm_link_columns)
                ]

                if prm_link_rows.any().any():
                    new_values_set = self._extract_multiple_and_single_values(
                        prm_link_rows["value"].convert_dtypes()
                    )

                    prm_link_value_by_id: dict[int, str] = (
                        self._replace_prm_link_id_to_value(
                            prm_link_ids=new_values_set
                        )
                    )

                    self._created_mo_prms.loc[
                        self._created_mo_prms["tprm_id"].isin(prm_link_columns),
                        "value",
                    ] = self._created_mo_prms.loc[
                        self._created_mo_prms["tprm_id"].isin(prm_link_columns),
                        "value",
                    ].apply(lambda x: prm_link_value_by_id.get(int(x), x))

                    del prm_link_value_by_id

            if self._create_object_parameters_and_attributes.any().any():
                columns_with_mo_link_values = (
                    self._create_object_parameters_and_attributes[
                        prm_link_columns
                    ]
                )

                if columns_with_mo_link_values.any().any():
                    all_prm_link_ids: set[int] = set()

                    for column_name in prm_link_columns:
                        mo_link_ids = self._extract_multiple_and_single_values(
                            columns_with_mo_link_values[
                                column_name
                            ].convert_dtypes()
                        )
                        all_prm_link_ids.update(mo_link_ids)

                    prm_link_value_by_id: dict[int, str] = (
                        self._replace_prm_link_id_to_value(
                            prm_link_ids=all_prm_link_ids
                        )
                    )

                    for column_name in prm_link_columns:
                        self._create_object_parameters_and_attributes[
                            column_name
                        ] = self._create_object_parameters_and_attributes[
                            column_name
                        ].apply(
                            self._old_replace_value_by_mappings,
                            mapping_values=prm_link_value_by_id,
                        )
                    del prm_link_value_by_id

            if self._deleted_object_values.any().any():
                prm_link_rows = self._deleted_object_values[
                    self._deleted_object_values["attr_name"].isin(
                        prm_link_columns
                    )
                ]

                if prm_link_rows.any().any():
                    new_values_set = self._extract_multiple_and_single_values(
                        prm_link_rows["value"].convert_dtypes()
                    )

                    prm_link_value_by_id: dict[int, str] = (
                        self._replace_prm_link_id_to_value(
                            prm_link_ids=new_values_set
                        )
                    )

                    self._deleted_object_values.loc[
                        self._deleted_object_values["attr_name"].isin(
                            prm_link_columns
                        ),
                        "old_value",
                    ] = self._deleted_object_values.loc[
                        self._deleted_object_values["attr_name"].isin(
                            prm_link_columns
                        ),
                        "old_value",
                    ].apply(lambda x: prm_link_value_by_id.get(int(x), x))

                    del prm_link_value_by_id

    def _points_column_converter(self, point_ids: set[int]) -> dict[int, str]:
        point_id_and_names = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(point_ids):
            if self._object_type_instance.points_constraint_by_tmo:
                stmt = select(MO.id, MO.name).where(
                    MO.id.in_(chunk),
                    MO.tmo_id.in_(
                        self._object_type_instance.points_constraint_by_tmo
                    ),
                )
            else:
                stmt = select(MO.id, MO.name).where(MO.id.in_(chunk))
            point_id_and_names.update(
                {
                    point_id: point_name
                    for point_id, point_name in self._session.exec(stmt).all()
                }
            )
        return point_id_and_names

    def _convert_parent_ids_to_parent_names(
        self, column: Literal["parent_name", "p_id"]
    ):
        if self._updated_object_attributes.any().any():
            parent_names = self._updated_object_attributes[
                self._updated_object_attributes["attr_name"] == column
            ]

            if parent_names.any().any():
                old_values_set = self._extract_single_values(
                    parent_names["old_value"]
                )
                new_values_set = self._extract_single_values(
                    parent_names["new_value"]
                )

                all_parent_names = old_values_set | new_values_set

                parent_name_by_id: dict[int, Any] = (
                    self._parent_id_column_converter(
                        parent_ids=all_parent_names
                    )
                )

                mask = self._updated_object_attributes["attr_name"] == column

                self._updated_object_attributes.loc[mask, "old_value"] = (
                    self._updated_object_attributes.loc[mask, "old_value"]
                    .astype("object")
                    .replace(parent_name_by_id)
                )

                self._updated_object_attributes.loc[mask, "new_value"] = (
                    self._updated_object_attributes.loc[mask, "new_value"]
                    .astype("object")
                    .replace(parent_name_by_id)
                )

                del parent_name_by_id

        if self._created_attributes.any().any():
            parent_names = self._created_attributes[
                self._created_attributes["attr_name"] == column
            ]

            if parent_names.any().any():
                parent_names["value"] = parent_names["value"].astype(int)
                new_values_set = self._extract_single_values(
                    parent_names["value"]
                )

                parent_name_by_id: dict[int, str] = (
                    self._parent_id_column_converter(parent_ids=new_values_set)
                )

                mask = self._created_attributes["attr_name"] == column

                self._created_attributes.loc[mask, "value"] = (
                    self._created_attributes.loc[mask, "value"]
                    .astype("object")
                    .replace(parent_name_by_id)
                )

                del parent_name_by_id

        if self._create_object_parameters_and_attributes.any().any():
            if column in self._create_object_parameters_and_attributes.columns:
                parent_column_values = (
                    self._create_object_parameters_and_attributes[column]
                )

                if parent_column_values.any().any():
                    parent_ids = parent_column_values.values.tolist()

                    parent_name_by_id: dict[int, str] = (
                        self._parent_id_column_converter(parent_ids=parent_ids)
                    )

                    self._create_object_parameters_and_attributes[column] = (
                        self._create_object_parameters_and_attributes[column]
                        .astype("object")
                        .apply(
                            self._old_replace_value_by_mappings,
                            mapping_values=parent_name_by_id,
                        )
                    )
                    del parent_name_by_id

        if self._deleted_object_values.any().any():
            parent_ids = self._deleted_object_values[
                self._deleted_object_values["attr_name"] == column
            ]
            if parent_ids.any().any():
                new_values_set = self._extract_single_values(
                    parent_ids["old_value"]
                )

                parent_name_by_id: dict[int, str] = (
                    self._parent_id_column_converter(parent_ids=new_values_set)
                )

                mask = self._deleted_object_values["attr_name"] == column

                self._deleted_object_values.loc[mask, "old_value"] = (
                    self._deleted_object_values.loc[mask, "old_value"]
                    .astype("object")
                    .replace(parent_name_by_id)
                )

                del parent_name_by_id

    def _convert_point_ids_to_point_names_in_dataframes(
        self,
        column: Literal[
            "point_a_name", "point_b_name", "point_a_id", "point_b_id"
        ],
    ):
        if self._updated_object_attributes.any().any():
            point_names = self._updated_object_attributes[
                self._updated_object_attributes["attr_name"] == column
            ]

            if point_names.any().any():
                old_values_set = self._extract_single_values(
                    point_names["old_value"]
                )
                new_values_set = self._extract_single_values(
                    point_names["new_value"]
                )

                all_point_names = old_values_set | new_values_set

                point_name_by_id: dict[int, Any] = (
                    self._points_column_converter(point_ids=all_point_names)
                )

                self._updated_object_attributes.loc[
                    self._updated_object_attributes["attr_name"] == column,
                    "old_value",
                ] = self._updated_object_attributes.loc[
                    self._updated_object_attributes["attr_name"] == column,
                    "old_value",
                ].replace(point_name_by_id)

                self._updated_object_attributes.loc[
                    self._updated_object_attributes["attr_name"] == column,
                    "new_value",
                ] = self._updated_object_attributes.loc[
                    self._updated_object_attributes["attr_name"] == column,
                    "new_value",
                ].replace(point_name_by_id)

                del point_name_by_id

        if self._created_attributes.any().any():
            point_names = self._created_attributes[
                self._created_attributes["attr_name"] == column
            ]

            if point_names.any().any():
                new_values_set = self._extract_single_values(
                    point_names["value"]
                )

                point_name_by_id: dict[int, str] = (
                    self._points_column_converter(point_ids=new_values_set)
                )

                self._created_attributes.loc[
                    self._created_attributes["attr_name"] == column, "value"
                ] = self._created_attributes.loc[
                    self._created_attributes["attr_name"] == column, "value"
                ].replace(point_name_by_id)

                del point_name_by_id

        if self._create_object_parameters_and_attributes.any().any():
            if column in self._create_object_parameters_and_attributes.columns:
                parent_column_values = (
                    self._create_object_parameters_and_attributes[column]
                )

                if parent_column_values.any().any():
                    point_ids = parent_column_values.values.tolist()

                    point_name_by_id: dict[int, str] = (
                        self._points_column_converter(point_ids=point_ids)
                    )

                    self._create_object_parameters_and_attributes[column] = (
                        self._create_object_parameters_and_attributes[
                            column
                        ].apply(
                            self._old_replace_value_by_mappings,
                            mapping_values=point_name_by_id,
                        )
                    )
                    del point_name_by_id

        if self._deleted_object_values.any().any():
            point_ids = self._deleted_object_values[
                self._deleted_object_values["attr_name"] == column
            ]
            if point_ids.any().any():
                new_values_set = self._extract_single_values(
                    point_ids["old_value"]
                )

                point_name_by_id: dict[int, str] = (
                    self._points_column_converter(point_ids=new_values_set)
                )

                self._deleted_object_values.loc[
                    self._deleted_object_values["attr_name"] == column,
                    "old_value",
                ] = self._deleted_object_values.loc[
                    self._deleted_object_values["attr_name"] == column,
                    "old_value",
                ].replace(point_name_by_id)

                del point_name_by_id

    def _get_parent_name_by_index_id(self):
        self._parent_name_by_index_id = {}
        self._parent_id_by_index_id = {}

        if "parent_name" in self._main_dataframe.columns:
            self._parent_id_by_index_id = self._main_dataframe.set_index(
                COLUMN_WITH_ORIGINAL_INDEXES
            )["parent_name"].to_dict()

        elif "p_id" in self._main_dataframe.columns:
            self._parent_id_by_index_id = self._main_dataframe.set_index(
                COLUMN_WITH_ORIGINAL_INDEXES
            )["p_id"].to_dict()

        unique_parent_ids = set(self._parent_id_by_index_id.values())
        if None in unique_parent_ids:
            unique_parent_ids.remove(None)

        parent_name_by_id = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(unique_parent_ids):
            stmt = select(MO.id, MO.name).where(MO.id.in_(chunk))
            parent_name_by_id.update(
                {
                    p_id: parent_name
                    for p_id, parent_name in self._session.exec(stmt).all()
                }
            )

        for index, p_id in self._parent_id_by_index_id.items():
            self._parent_name_by_index_id[index] = parent_name_by_id.get(p_id)

    def execute(self) -> io.BytesIO:
        self.validate_file_data()
        self._convert_mo_links_ids_to_mo_names_in_dataframes()
        self._convert_prm_links_ids_to_values_in_dataframes()

        for point_instance in [
            "point_a_name",
            "point_b_name",
            "point_a_id",
            "point_b_id",
        ]:
            self._convert_point_ids_to_point_names_in_dataframes(
                column=point_instance  # noqa
            )

        for parent_instance in ["parent_name", "p_id"]:
            self._convert_parent_ids_to_parent_names(column=parent_instance)  # noqa

        # data collection
        self._get_object_names_by_index()
        self._get_parent_name_by_index_id()

        self._get_error_values_by_column_names()

        self._get_data_statistic()

        self._replace_value_in_dataframe(
            dataframe=self._main_dataframe, old_value=np.nan, new_value=None
        )

        self._format_sheets_for_xlsx_file()

        self._workbook.close()

        self._output.seek(0)

        return self._output


class ReportBase:
    @staticmethod
    def add_data_to_workbook_from_dataframe(
        worksheet: Worksheet, dataframe: DataFrame
    ) -> Worksheet:
        """Adds DataFrame values to an already created worksheet."""
        for col_num, column_name in enumerate(dataframe.columns):
            worksheet.write(0, col_num, column_name)

        # Write the data rows, converting list/dict values to JSON strings
        for row_num, row in enumerate(
            dataframe.itertuples(index=False, name=None), start=1
        ):
            for col_num, value in enumerate(row):
                if isinstance(value, (list, dict)):
                    worksheet.write(row_num, col_num, json.dumps(value))
                else:
                    worksheet.write(row_num, col_num, value)
        return worksheet

    @staticmethod
    def _remove_columns_from_dataframe(
        column_names_to_delete: list[str], dataframe: DataFrame
    ) -> DataFrame:
        existing_columns_to_delete = [
            col for col in column_names_to_delete if col in dataframe.columns
        ]

        if existing_columns_to_delete:
            dataframe = dataframe.drop(columns=existing_columns_to_delete)
        return dataframe

    @staticmethod
    def _replace_value_in_dataframe(
        dataframe: DataFrame, old_value: Any, new_value: Any
    ) -> DataFrame:
        dataframe.replace({old_value: new_value}, inplace=True)
        return dataframe


class SummaryReport(ReportBase):
    def __init__(
        self,
        sheet_name: str,
        workbook: xlsxwriter.Workbook,
        data_statistic: dict,
    ) -> None:
        self.workbook = workbook
        self.sheet_name = sheet_name
        self.data_statistic = data_statistic

    def generate(self) -> xlsxwriter.Workbook:
        """
        This method get all counted data by CUD
        statistic and fill worksheet by this data
        """
        data = self.data_statistic
        worksheet = self.workbook.add_worksheet(self.sheet_name)

        for col_num, key in enumerate(data.keys()):
            worksheet.write(0, col_num, key)

        for col_num, value in enumerate(data.values()):
            worksheet.write(1, col_num, value)

        return self.workbook


class DataModelReport(ReportBase):
    def __init__(
        self,
        session: Session,
        sheet_name: str,
        workbook: xlsxwriter.Workbook,
        dominant_types: List[str],
        new_column_name_mapping: dict[str, str],
        error_instances: ErrorInstances,
        tprm_instance_by_id: dict[int, TPRM],
    ) -> None:
        self.session = session
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.dominant_types = dominant_types
        self.new_column_name_mapping = new_column_name_mapping
        self.error_instances = error_instances
        self.tprm_instance_by_id = tprm_instance_by_id

    def generate(self) -> xlsxwriter.Workbook:
        headers = [
            "File column name",
            "File column type",
            "Inventory column name",
            "Inventory column type",
            "Constraint",
            "Required",
            "Status",
        ]
        worksheet = self.workbook.add_worksheet(self.sheet_name)

        dominant_types: List[str] = self.dominant_types
        data_to_fill = list()
        all_data = pd.DataFrame()

        for column_index, (inventory_column, file_column) in enumerate(
            self.new_column_name_mapping.items(), start=1
        ):
            status = self._get_column_status(inventory_column)
            column_val_type, constraint, required = self._get_column_type_info(
                inventory_column
            )

            if inventory_column.isdigit():
                inventory_column = self.tprm_instance_by_id[
                    int(inventory_column)
                ].name

            file_column_type = dominant_types[column_index - 1]
            # format row
            row_data = [
                file_column,
                file_column_type,
                inventory_column,
                column_val_type,
                constraint,
                required,
                status.value,
            ]

            data_to_fill.append(row_data)

        new_rows_df = pd.DataFrame(data=data_to_fill, columns=headers)
        all_data = pd.concat(objs=[all_data, new_rows_df], ignore_index=True)

        self.add_data_to_workbook_from_dataframe(
            worksheet=worksheet, dataframe=all_data
        )
        return self.workbook

    def _get_column_status(self, column: str) -> WarningStatuses:
        """
        Returns the status of the given inventory column
        based on errors and warnings.
        """
        if self.error_instances.errors.get(column):
            return WarningStatuses.error
        elif self.error_instances.warnings.get(column):
            return WarningStatuses.warning
        return WarningStatuses.ok

    def _get_column_type_info(
        self, column: str
    ) -> tuple[str, Optional[str], Optional[bool]]:
        """
        Returns the column value type, constraint, and
        required flag for the given inventory column.
        """
        mo_attr_val_types_mapping = {
            "active": "bool",
            "status": "str",
            "parent_name": "str",
            "p_id": "int",
            "point_a_name": "str",
            "point_b_name": "str",
            "pov": "json",
            "geometry": "json",
        }

        if column.isdigit() and int(column) in self.tprm_instance_by_id:
            tprm = self.tprm_instance_by_id[int(column)]
            if tprm.val_type == "mo_link" and tprm.constraint:
                constraint = self.session.exec(
                    select(TMO.name).where(TMO.id == int(tprm.constraint))
                ).first()
                constraint = f'Object type "{constraint}"'

            elif tprm.val_type == "prm_link" and tprm.constraint:
                constraint = self.session.exec(
                    select(TPRM.name).where(TPRM.id == int(tprm.constraint))
                ).first()
                constraint = f'Parameter type "{constraint}"'

            else:
                constraint = tprm.constraint

            return tprm.val_type, constraint, tprm.required

        return mo_attr_val_types_mapping.get(column, "str"), None, None


class CreateSheetReport(ReportBase):
    def __init__(
        self,
        sheet_name: str,
        workbook: xlsxwriter.Workbook,
        new_column_name_mapping: dict[str, str],
        object_name_by_id: dict[int, str],
        parent_name_by_id: dict[int, str],
        error_instances: ErrorInstances,
        create_mo_and_prm_and_attr: DataFrame,
    ) -> None:
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.new_column_name_mapping = new_column_name_mapping
        self.object_name_by_id = object_name_by_id
        self.parent_name_by_id = parent_name_by_id
        self.error_instances = error_instances
        self.create_mo_and_prm_and_attr = create_mo_and_prm_and_attr
        self._warning_pattern_color = {"bg_color": "yellow"}

    def generate(self) -> xlsxwriter.Workbook:
        worksheet = self.workbook.add_worksheet(self.sheet_name)

        headers = ["Parent Name", "Object Name"]
        all_data = pd.DataFrame()

        if not self.create_mo_and_prm_and_attr.empty:
            mo_data = self._clean_mo_data()

            headers += self._generate_headers(mo_data)

            rows = [self._create_row(row) for _, row in mo_data.iterrows()]

            new_dataframe = pd.DataFrame(data=rows, columns=headers)
            all_data = pd.concat(
                objs=[all_data, new_dataframe], ignore_index=True
            )

            all_data = all_data.sort_values(by="Object Name").reset_index(
                drop=True
            )

        self._replace_value_in_dataframe(all_data, np.nan, None)
        self._apply_warning_formatting(worksheet=worksheet, data=all_data)

        return self.workbook

    def _clean_mo_data(self):
        """Removes unnecessary columns from the provided data."""
        columns_to_remove = ["p_id", "parent_name", COMBINED_NAMES_COLUMN]
        return self._remove_columns_from_dataframe(
            column_names_to_delete=columns_to_remove,
            dataframe=self.create_mo_and_prm_and_attr,
        )

    def _generate_headers(self, mo_data: pd.DataFrame):
        """Generates headers for the worksheet based on column mappings."""
        return [
            self.new_column_name_mapping.get(col, col)
            for col in mo_data.columns
        ]

    def _create_row(self, row: pd.Series) -> list:
        """Creates a properly structured row for the worksheet."""
        original_index = row[COLUMN_WITH_ORIGINAL_INDEXES]

        mo_name = self.object_name_by_id.get(original_index)
        parent_name = self.parent_name_by_id.get(original_index)

        row_data = row.values.tolist()

        return [parent_name, mo_name] + row_data

    def _apply_warning_formatting(
        self, worksheet: xlsxwriter.workbook.Worksheet, data: pd.DataFrame
    ):
        """
        Colorizes cells in the worksheet if the value in the warning column or the original
        indexes matches the warning index, using xlsxwriter.

        The `data` parameter is expected to be a DataFrame where the rows contain the data to be written
        and potentially colorized based on warnings.
        """
        warning_format = self.workbook.add_format(self._warning_pattern_color)

        headers = list(data.columns)
        header_of_worksheet = {
            header: idx for idx, header in enumerate(headers)
        }

        inventory_column_name_by_aliased = {
            v: k for k, v in self.new_column_name_mapping.items()
        }

        column_index_with_original_indexes = header_of_worksheet.get(
            COLUMN_WITH_ORIGINAL_INDEXES
        )

        for col_num, header in enumerate(headers):
            if header == COLUMN_WITH_ORIGINAL_INDEXES:
                continue  # Skip this column
            adjusted_col_num = (
                col_num
                if col_num < column_index_with_original_indexes
                else col_num - 1
            )
            worksheet.write(0, adjusted_col_num, header)

        for row_idx, row_data in data.iterrows():
            original_index = row_data[COLUMN_WITH_ORIGINAL_INDEXES]

            for col_name, col_value in row_data.items():
                if col_name == COLUMN_WITH_ORIGINAL_INDEXES:
                    continue

                if isinstance(col_value, (dict, list)):
                    col_value = json.dumps(col_value)
                col_num = header_of_worksheet[col_name]
                inventory_column_name_view = (
                    inventory_column_name_by_aliased.get(col_name)
                )

                adjusted_col_num = (
                    col_num
                    if col_num < column_index_with_original_indexes
                    else col_num - 1
                )

                if inventory_column_name_view in self.error_instances.warnings:
                    warning_data = self.error_instances.warnings[
                        inventory_column_name_view
                    ]
                    warning_indexes = {
                        item.index_of_error_value for item in warning_data
                    }

                    if original_index in warning_indexes:
                        worksheet.write(
                            row_idx + 1,
                            adjusted_col_num,
                            col_value,
                            warning_format,
                        )
                    else:
                        worksheet.write(
                            row_idx + 1, adjusted_col_num, col_value
                        )
                else:
                    worksheet.write(row_idx + 1, adjusted_col_num, col_value)

        return worksheet


class ErrorSheetReport(ReportBase):
    def __init__(
        self,
        sheet_name: str,
        workbook: xlsxwriter.Workbook,
        new_column_name_mapping: dict[str, str],
        error_instances: ErrorInstances,
        object_name_by_id: dict[int, str],
        parent_name_by_id: dict[int, str],
        primary_tprms: list[str],
    ) -> None:
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.new_column_name_mapping = new_column_name_mapping
        self.error_instances = error_instances
        self.object_name_by_id = object_name_by_id
        self.parent_name_by_id = parent_name_by_id
        self.primary_tprms = primary_tprms

    def generate(self) -> xlsxwriter.Workbook:
        worksheet = self.workbook.add_worksheet(self.sheet_name)

        headers_for_error_file = [
            "Parent Name",
            "Object Name",
            "Parameter Name",
            "Value",
            "Reason",
            "Row ID",
        ]

        data_to_fill = self._collect_error_data()

        all_data = pd.DataFrame(data_to_fill, columns=headers_for_error_file)
        all_data = all_data.sort_values(by="Object Name").reset_index(drop=True)

        self.add_data_to_workbook_from_dataframe(
            worksheet=worksheet, dataframe=all_data
        )

        return self.workbook

    def _collect_error_data(self):
        """Collects error data and returns it as a list of rows."""
        data_to_fill = []

        for column, error_instances in self.error_instances.errors.items():
            column_name_for_user_view = self.new_column_name_mapping.get(
                column, column
            )

            for error_instance in error_instances:
                new_row = self._create_error_row(
                    error_instance, column_name_for_user_view
                )
                data_to_fill.append(new_row)

        return data_to_fill

    def _create_error_row(self, error_instance, column_name_for_user_view):
        """Creates a single row of error data."""
        object_name = self.object_name_by_id.get(
            error_instance.index_of_error_value
        )
        parent_name = self.parent_name_by_id.get(
            error_instance.index_of_error_value
        )
        original_value = error_instance.error_value
        reason = self._get_error_reason(
            error_instance, column_name_for_user_view
        )
        row_id = error_instance.index_of_error_value + 1

        return [
            parent_name,
            object_name,
            column_name_for_user_view,
            original_value,
            reason,
            row_id,
        ]

    def _get_error_reason(self, error_instance, column_name):
        if column_name.isdigit() and int(column_name) in self.primary_tprms:
            return get_primary_reason(
                row_id=error_instance.index_of_error_value,
                reason=reason_status_to_message[error_instance.status],
            )
        return reason_status_to_message.get(
            error_instance.status, error_instance.status
        )


class UpdateSheetReport(ReportBase):
    def __init__(
        self,
        sheet_name: str,
        workbook: xlsxwriter.Workbook,
        new_column_name_mapping: dict[str, str],
        object_name_by_id: dict[int, str],
        parent_name_by_id: dict[int, str],
        tprm_instance_by_id: dict[int, TPRM],
        primary_tprms: list[str],
        mo_data: dict[str, DataFrame],
    ) -> None:
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.new_column_name_mapping = new_column_name_mapping
        self.object_name_by_id = object_name_by_id
        self.parent_name_by_id = parent_name_by_id
        self.tprm_instance_by_id = tprm_instance_by_id
        self.primary_tprms = primary_tprms
        self.updated_mo_attrs = mo_data.get("updated_mo_attrs", pd.DataFrame())
        self.updated_mo_prms = mo_data.get("updated_mo_prms", pd.DataFrame())
        self.created_mo_attrs = mo_data.get("created_mo_attrs", pd.DataFrame())
        self.created_mo_prms = mo_data.get("created_mo_prms", pd.DataFrame())

    def generate(self) -> xlsxwriter.Workbook:
        worksheet = self.workbook.add_worksheet(self.sheet_name)

        headers = [
            "Parent Name",
            "Object Name",
            "Parameter Name",
            "Old Value",
            "New Value",
        ]
        updated_data = list()

        all_tprm_names, primary_tprm_name_with_index = (
            self._prepare_tprm_mappings()
        )

        formatted_updated_attrs = self._process_dataframe(
            dataframe=self.updated_mo_attrs
        )
        formatted_updated_paramters = self._process_dataframe(
            dataframe=self.updated_mo_prms,
            primary_tprm_name_with_index=primary_tprm_name_with_index,
            is_param=True,
        )
        formatted_created_paramters = self._process_dataframe(
            dataframe=self.created_mo_prms,
            primary_tprm_name_with_index=primary_tprm_name_with_index,
            is_param=True,
            created=True,
        )
        formatted_created_attributes = self._process_dataframe(
            dataframe=self.created_mo_attrs, created=True
        )

        updated_data.extend(formatted_updated_attrs)
        updated_data.extend(formatted_updated_paramters)
        updated_data.extend(formatted_created_paramters)
        updated_data.extend(formatted_created_attributes)

        all_data = pd.DataFrame(updated_data, columns=headers)
        all_data = all_data.sort_values(by="Object Name").reset_index(drop=True)

        self._replace_value_in_dataframe(all_data, np.nan, None)
        self.add_data_to_workbook_from_dataframe(worksheet, all_data)
        return self.workbook

    def _prepare_tprm_mappings(self) -> tuple[dict, dict]:
        """Prepare TPRM name mappings."""
        all_tprm_names = {
            tprm.id: tprm.name for tprm in self.tprm_instance_by_id.values()
        }
        primary_tprm_name_with_index = {
            tprm.name: self.primary_tprms.index(str(tprm.id))
            for tprm in self.tprm_instance_by_id.values()
            if tprm.id in self.primary_tprms
        }
        return all_tprm_names, primary_tprm_name_with_index

    def _process_dataframe(
        self,
        dataframe: DataFrame,
        primary_tprm_name_with_index: dict[str, int] | None = None,
        is_param: bool = False,
        created: bool = False,
    ) -> list:
        """Process a DataFrame and extract data into a list."""
        result = list()

        if dataframe.empty:
            return result

        for _, item in dataframe.iterrows():
            original_index = item[COLUMN_WITH_ORIGINAL_INDEXES]
            new_value = item["value"] if created else item["new_value"]
            old_value = None if created else item.get("old_value")

            attr_name = self._get_attribute_name(item, is_param)

            parent_name = self.parent_name_by_id.get(original_index)
            object_name = self.object_name_by_id.get(original_index)

            if (
                is_param
                and primary_tprm_name_with_index
                and attr_name in primary_tprm_name_with_index
            ):
                index = primary_tprm_name_with_index[attr_name]
                object_name = self._update_object_name(
                    object_name, new_value, index
                )

            result.append(
                (parent_name, object_name, attr_name, old_value, new_value)
            )

        return result

    def _get_attribute_name(self, item: pd.Series, is_param: bool) -> str:
        """Retrieve the attribute or parameter name with optional mapping."""
        if is_param:
            return self.new_column_name_mapping.get(
                item["tprm_id"], item["tprm_id"]
            )
        return self.new_column_name_mapping.get(
            item["attr_name"], item["attr_name"]
        )

    def _update_object_name(
        self, object_name: str, new_value: str, index: int
    ) -> str:
        """Update object name by inserting new value at the given index."""
        object_name_parts = object_name.split(NAME_DELIMITER)
        object_name_parts[index] = new_value
        return NAME_DELIMITER.join(object_name_parts)


@dataclass
class ConvertedMultipleValues:
    list_view_values: list[Any]
    error_values: Any
    error_indexes: list[int]


def calculate_dominant_type(column: pd.Series) -> str:
    init_type_counter = Counter()

    for value in column:
        if pd.isna(value):
            continue
        if isinstance(value, str):
            try:
                parsed_value = literal_eval(value)
                if isinstance(parsed_value, dict):
                    init_type_counter["json"] += 1
                    continue
                elif isinstance(parsed_value, list):
                    init_type_counter["list"] += 1
                    continue
                elif isinstance(parsed_value, bool):
                    init_type_counter["bool"] += 1
                    continue
                elif isinstance(parsed_value, int):
                    init_type_counter["int"] += 1
                    continue
                elif isinstance(parsed_value, float):
                    init_type_counter["float"] += 1
                    continue
            except (ValueError, SyntaxError):
                pass

            if value.lower() in {"true", "false"}:
                init_type_counter["bool"] += 1

            elif "." in value:
                try:
                    float(value)
                    init_type_counter["float"] += 1
                except ValueError:
                    init_type_counter["str"] += 1

            else:
                try:
                    int(value)
                    init_type_counter["int"] += 1
                except ValueError:
                    init_type_counter["str"] += 1

    if init_type_counter:
        return init_type_counter.most_common(1)[0][0]
    return None


class MultipleTPRMValidation(BatchImportValidator):
    def __init__(
        self, session: Session, tprm_instance: TPRM, values_with_indexes: Series
    ):
        self._session = session
        self._tprm_instance = tprm_instance

        self._INDEX_COLUMN_NAME = "indexes"
        self._VALUES_COLUMN_NAME = "values"
        self._dataframe_for_validation = DataFrame(
            {
                self._INDEX_COLUMN_NAME: values_with_indexes.index.tolist(),
                self._VALUES_COLUMN_NAME: values_with_indexes.values.tolist(),
            }
        )

        self._errors: list[BatchPreviewErrorInstance] = []

    def _multiple_str_column_validation(self):
        errors = []
        warnings = []

        if self._tprm_instance.constraint:

            def check_str_constraint(multiple_value, pattern: re.Pattern[str]):
                for value in multiple_value:
                    if not isinstance(value, str) or not pattern.match(value):
                        return np.nan
                return True

            pattern = re.compile(rf"{self._tprm_instance.constraint}")
            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(check_str_constraint, pattern=pattern)

            not_valid_constraint_indexes = validation_result[
                validation_result.isna()
            ].index.tolist()
            if not_valid_constraint_indexes:
                not_valid_constraint_values = (
                    self._get_values_from_dataframe_by_indexes(
                        dataframe=self._dataframe_for_validation,
                        column_name=self._VALUES_COLUMN_NAME,
                        indexes_to_search=not_valid_constraint_indexes,
                    )
                )

                errors.extend(
                    self._format_error_structure_for_batch_preview(
                        error_values=not_valid_constraint_values,
                        error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                        error_indexes=not_valid_constraint_indexes,
                    )
                )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_int_column_validation(self):
        errors = []
        warnings = []

        def check_multiple_int_column_type(multiple_value):
            """
            This method returns:
            -- True: if every value is int
            -- False: if there are "intable string" or float
            -- Nan: if value can't be converted to int
            """
            warning = False
            try:
                for value in multiple_value:
                    if isinstance(value, float) or isinstance(value, bool):
                        warning = True
                    elif isinstance(value, str) and value.lower() in {
                        "true",
                        "false",
                    }:
                        warning = True
                    elif isinstance(value, int):
                        continue

                    # its error value
                    else:
                        return np.nan

                if warning:
                    return False
                return True

            except (TypeError, ValueError):
                return np.nan

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_multiple_int_column_type)

        not_multiple_values_indexes = validation_result[
            validation_result.isna()
        ].index.tolist()
        if not_multiple_values_indexes:
            not_valid_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_multiple_values_indexes,
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_values,
                    error_status=NOT_VALID_VALUE_TYPE,
                    error_indexes=not_multiple_values_indexes,
                )
            )

            self._dataframe_for_validation.drop(
                not_multiple_values_indexes, inplace=True
            )

        warning_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        if warning_indexes:
            warning_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=warning_indexes,
            )

            warnings.extend(
                self._format_warning_structure_for_batch_preview(
                    warning_values=warning_values,
                    warning_indexes=warning_indexes,
                )
            )

        if self._tprm_instance.constraint:
            bottom, top = self._tprm_instance.constraint.split(":")
            top = int(top)
            bottom = int(bottom)

            def check_int_constraint(multiple_value, top: int, bottom: int):
                if not multiple_value:
                    return multiple_value

                if all(bottom <= int(value) <= top for value in multiple_value):
                    return True

                return np.nan

            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(check_int_constraint, top=top, bottom=bottom)
            not_valid_constraint_indexes = validation_result[
                validation_result.isna()
            ].index.tolist()

            not_valid_constraint_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_valid_constraint_indexes,
                )
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_constraint_values,
                    error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                    error_indexes=not_valid_constraint_indexes,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_float_column_validation(self):
        errors = []
        warnings = []

        def check_multiple_float_column_type(multiple_value):
            """
            Return list of converted value if value is valid,
            in other case return Nan
            """
            if not multiple_value:
                return multiple_value
            try:
                result = []
                for value in multiple_value:
                    result.append(float(value))
                return result

            except ValueError:
                return "FALSE"

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_multiple_float_column_type)

        not_multiple_values_indexes = validation_result[
            validation_result == "FALSE"
        ].index.tolist()
        not_multiple_values_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_multiple_values_indexes,
        )

        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_multiple_values_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_multiple_values_indexes,
            )
        )

        self._dataframe_for_validation.drop(
            not_multiple_values_indexes, inplace=True
        )

        if self._tprm_instance.constraint:
            bottom, top = self._tprm_instance.constraint.split(":")

            def check_float_constraint(
                multiple_value, top: float, bottom: float
            ):
                if not multiple_value:
                    return "FALSE"

                if all(bottom <= value <= top for value in multiple_value):
                    return True
                return np.nan

            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(
                check_float_constraint, top=float(top), bottom=float(bottom)
            )

            not_valid_constraint_index = validation_result[
                validation_result.isna()
            ].index.tolist()
            not_valid_constraint_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_valid_constraint_index,
                )
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_constraint_values,
                    error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                    error_indexes=not_valid_constraint_index,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_date_column_validation(self):
        errors = []
        warnings = []
        date_format = "%Y-%m-%d"

        def check_multiple_date_column_type(multiple_value, date_format: str):
            if multiple_value:
                try:
                    for value in multiple_value:
                        datetime.strptime(value, date_format)
                    return True

                except (ValueError, TypeError):
                    return np.nan

            return True

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_multiple_date_column_type, date_format=date_format)
        not_valid_type_indexes = validation_result[
            validation_result.isna()
        ].index.tolist()

        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )

        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_datetime_column_validation(self):
        errors = []
        warnings = []

        def check_multiple_datetime_column_type(multiple_value):
            allowed_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]

            if multiple_value:
                for value in multiple_value:
                    matched_format = False
                    for datetime_format in allowed_formats:
                        try:
                            datetime.strptime(value, datetime_format)
                            matched_format = True
                            break
                        except (TypeError, ValueError):
                            continue
                    if not matched_format:
                        return False
                return True

            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(
            check_multiple_datetime_column_type,
        )

        not_valid_value_type_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()

        not_valid_value_type_values = (
            self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_valid_value_type_indexes,
            )
        )

        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_value_type_values,
                error_status=NOT_MULTIPLE_VALUE,
                error_indexes=not_valid_value_type_indexes,
            )
        )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_bool_column_validation(self):
        errors = []
        warnings = []

        def check_multiple_bool_column_type_errors_and_warnings(multiple_value):
            """
            Return:
                 -- Nan. If value is not multiple

                 -- 0. If there are values, which are warning ('0' or '1')
                 -- 1. If there are values which can't be bool
                 -- 2. If every value is valid

            """
            allowed_values = {"true", "false"}
            warning_values = {"0", "1"}

            warning = False
            try:
                if multiple_value:
                    for value in multiple_value:
                        value = str(value).lower()
                        if value.lower() in allowed_values:
                            continue
                        elif value in warning_values:
                            warning = True
                        else:
                            return 1

                    return 0 if warning else 2
                return 2

            except (ValueError, TypeError):
                return np.nan

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_multiple_bool_column_type_errors_and_warnings)
        not_multiple_value_indexes = validation_result[
            validation_result.isna()
        ].index.tolist()
        not_valid_type_indexes = validation_result[
            validation_result == 1
        ].index.tolist()
        warning_bool_values_indexes = validation_result[
            validation_result == 0
        ].index.tolist()

        if not_multiple_value_indexes:
            not_multiple_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_multiple_value_indexes,
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_multiple_values,
                    error_status=NOT_MULTIPLE_VALUE,
                    error_indexes=not_multiple_value_indexes,
                )
            )

        if not_valid_type_indexes:
            not_valid_type_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_valid_type_indexes,
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_type_values,
                    error_status=NOT_VALID_VALUE_TYPE,
                    error_indexes=not_valid_type_indexes,
                )
            )

        if warning_bool_values_indexes:
            warning_bool_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=warning_bool_values_indexes,
            )

            warnings.extend(
                self._format_warning_structure_for_batch_preview(
                    warning_values=warning_bool_values,
                    warning_indexes=warning_bool_values_indexes,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_mo_link_column_validation(self):
        errors = []
        warnings = []

        all_mo_link_names = []
        for value in self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].tolist():
            if value:
                all_mo_link_names.extend(value)

        all_mo_link_names = set(all_mo_link_names)
        exists_object_names = []

        if self._tprm_instance.constraint:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                all_mo_link_names
            ):
                stmt = select(MO.name).where(
                    MO.name.in_(chunk),
                    MO.tmo_id == int(self._tprm_instance.constraint),
                )
                exists_object_names.extend(
                    mo_name
                    for mo_name in self._session.execute(stmt).scalars().all()
                )
        else:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                all_mo_link_names
            ):
                stmt = select(MO.name).where(MO.name.in_(chunk))
                exists_object_names.extend(
                    mo_name
                    for mo_name in self._session.execute(stmt).scalars().all()
                )

        not_exists_values = all_mo_link_names.difference(
            set(exists_object_names)
        )

        def check_intersection_between_sets(
            value: str, values_to_check: set[str]
        ):
            """
            If sets has same values - it return Nan, in they are different -  it returns True
            """
            if value:
                if set(value).intersection(values_to_check):
                    return np.nan
            return True

        if not_exists_values:
            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(
                check_intersection_between_sets,
                values_to_check=not_exists_values,
            )
            not_exists_objects_indexes = validation_result[
                validation_result.isna()
            ].index.tolist()
            not_exists_objects_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_exists_objects_indexes,
                )
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_objects_values,
                    error_status=NOT_EXISTS_OBJECTS,
                    error_indexes=not_exists_objects_indexes,
                )
            )
            self._dataframe_for_validation.drop(
                not_exists_objects_indexes, inplace=True
            )

        series = pd.Series(exists_object_names)
        not_concrete_names = series[series.duplicated()].unique().tolist()
        if not_concrete_names:
            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(
                check_intersection_between_sets,
                values_to_check=set(not_concrete_names),
            )
            not_exists_objects_indexes = validation_result[
                validation_result.isna()
            ].index.tolist()
            not_exists_objects_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_exists_objects_indexes,
                )
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_objects_values,
                    error_status=NOT_CONCRETE_NAME,
                    error_indexes=not_exists_objects_indexes,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_prm_link_column_validation(self):
        errors = []
        warnings = []

        prm_link_values = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ]

        all_prm_links = []
        for value in prm_link_values.values.tolist():
            if value:
                all_prm_links.extend(value)

        all_prm_links = set(all_prm_links)
        exists_values = []

        for chunk in get_chunked_values_by_sqlalchemy_limit(all_prm_links):
            stmt = select(PRM.value).where(
                PRM.value.in_([str(_) for _ in chunk]),
                PRM.tprm_id == int(self._tprm_instance.constraint),
            )
            exists_values.extend(self._session.execute(stmt).scalars().all())

        not_exists_values = all_prm_links.difference(set(exists_values))

        if not_exists_values:

            def check_intersection_between_exists_and_not_exists_values(
                value: list[str], not_exists_values: set[str]
            ):
                if value and set(value).intersection(not_exists_values):
                    return np.nan
                return True

            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(
                check_intersection_between_exists_and_not_exists_values,
                not_exists_values=not_exists_values,
            )
            not_exists_values_indexes = validation_result[
                validation_result.isna()
            ].index.tolist()

            not_exists_values_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_exists_values_indexes,
                )
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_values_values,
                    error_status=NOT_EXISTS_OBJECTS,
                    error_indexes=not_exists_values_indexes,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _multiple_two_way_mo_link_column_validation(self):
        return self._multiple_mo_link_column_validation()

    @staticmethod
    def _convert_multiple_values_from_string_view_to_list(
        series_with_values: Series,
    ):
        def check_multiple_column_type(value):
            if value:
                try:
                    converted_values = literal_eval(value)
                    if isinstance(converted_values, list):
                        return converted_values
                    raise ValueError

                except (SyntaxError, ValueError):
                    return False

            return None

        converted_to_lists = series_with_values.apply(
            check_multiple_column_type
        )

        not_valid_items = converted_to_lists[converted_to_lists == False]  # noqa
        valid_values = converted_to_lists[
            converted_to_lists != False  # noqa
        ].values.tolist()

        not_valid_type_indexes = not_valid_items.index.tolist()
        not_valid_type_values = []

        if not_valid_type_indexes:
            not_valid_type_values = series_with_values.loc[
                not_valid_type_indexes
            ].tolist()

        return ConvertedMultipleValues(
            list_view_values=valid_values,
            error_indexes=not_valid_type_indexes,
            error_values=not_valid_type_values,
        )

    def _convert_string_values_to_lists(self):
        """
        This method convert string values to list using literal eval
        + validate to error "not list" view values
        """
        errors = []

        converted_values = (
            self._convert_multiple_values_from_string_view_to_list(
                series_with_values=self._dataframe_for_validation[
                    self._VALUES_COLUMN_NAME
                ]
            )
        )

        if converted_values.error_indexes:
            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=converted_values.error_values,
                    error_status=NOT_MULTIPLE_VALUE,
                    error_indexes=converted_values.error_indexes,
                )
            )
            self._dataframe_for_validation.drop(
                converted_values.error_indexes, inplace=True
            )

        self._dataframe_for_validation[self._VALUES_COLUMN_NAME] = (
            converted_values.list_view_values
        )

        self._errors.extend(errors)
        return ConvertedToListValuesInColumn(
            column_with_converted_values=self._dataframe_for_validation,
            errors=errors,
        )

    def _check_required_values_in_multiple_values(self):
        """
        This method checks is required valuse are None, or in list of values are None, empty
        values
        """
        errors = []
        if self._tprm_instance.required:

            def validate_empty_values_in_list(list_of_values_to_check):
                empty_values = [None, ""]

                if list_of_values_to_check is None:
                    return False

                if any(
                    value in empty_values for value in list_of_values_to_check
                ):
                    return False

                return True

            values_to_validate = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ]
            validation_result = values_to_validate.apply(
                validate_empty_values_in_list
            )
            empty_values_indexes = validation_result[
                validation_result == False  # noqa
            ].index.tolist()

            if empty_values_indexes:
                empty_values_values = (
                    self._get_values_from_dataframe_by_indexes(
                        dataframe=self._dataframe_for_validation,
                        column_name=self._VALUES_COLUMN_NAME,
                        indexes_to_search=empty_values_indexes,
                    )
                )

                errors.extend(
                    self._format_error_structure_for_batch_preview(
                        error_values=empty_values_values,
                        error_status=EMPTY_VALUE_IN_REQUIRED,
                        error_indexes=empty_values_indexes,
                    )
                )

                self._dataframe_for_validation.drop(
                    empty_values_indexes, inplace=True
                )

        self._errors.extend(errors)

        return ValidatedRequiredColumn(
            validated_dataframe=self._dataframe_for_validation, errors=errors
        )

    def _multiple_enum_column_validation(self):
        errors = []
        warnings = []

        def check_multiple_enum_column_values_constraint(
            multiple_value, constraint: list[Any]
        ):
            if multiple_value:
                for value in multiple_value:
                    if value in constraint:
                        continue
                    return False
                return True

            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(
            check_multiple_enum_column_values_constraint,
            constraint=[
                str(v) for v in literal_eval(self._tprm_instance.constraint)
            ],
        )
        not_valid_constraint_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()

        not_valid_constraint_values = (
            self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_valid_constraint_indexes,
            )
        )

        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_constraint_values,
                error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                error_indexes=not_valid_constraint_indexes,
            )
        )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _validate_tprm_values_by_val_type(self, val_type: str):
        validation_methods_by_val_type: dict[str, func] = {
            "str": self._multiple_str_column_validation,
            "int": self._multiple_int_column_validation,
            "float": self._multiple_float_column_validation,
            "date": self._multiple_date_column_validation,
            "datetime": self._multiple_datetime_column_validation,
            "bool": self._multiple_bool_column_validation,
            "mo_link": self._multiple_mo_link_column_validation,
            "prm_link": self._multiple_prm_link_column_validation,
            enum_val_type_name: self._multiple_enum_column_validation,
            two_way_mo_link_val_type_name: self._multiple_two_way_mo_link_column_validation,
        }

        validation_method = validation_methods_by_val_type.get(val_type)

        if validation_method:
            error_and_warnings = validation_method()
            return error_and_warnings

        return ErrorsAndWarnings(errors=[], warnings=[])

    def validate(self) -> ErrorsAndWarnings:
        self._convert_string_values_to_lists()
        self._check_required_values_in_multiple_values()

        error_and_warnings: ErrorsAndWarnings = (
            self._validate_tprm_values_by_val_type(
                val_type=self._tprm_instance.val_type
            )
        )

        error_and_warnings.errors.extend(self._errors)

        del self._dataframe_for_validation
        return error_and_warnings


class SingleTPRMValidation(BatchImportValidator):
    def __init__(
        self,
        session: Session,
        tmo_instance: TMO,
        tprm_instance: TPRM,
        values_with_indexes: Series,
    ):
        self._session = session
        self._tmo_instance = tmo_instance
        self._tprm_instance = tprm_instance

        self._INDEX_COLUMN_NAME = "indexes"
        self._VALUES_COLUMN_NAME = "values"
        self._dataframe_for_validation = DataFrame(
            {
                self._INDEX_COLUMN_NAME: values_with_indexes.index,
                self._VALUES_COLUMN_NAME: values_with_indexes.values,
            }
        )

        self._errors: list[BatchPreviewErrorInstance] = []

        self._tprm_is_primary: bool = (
            self._tprm_instance.id in self._tmo_instance.primary
        )

    def _str_column_validation(self):
        errors = []
        warnings = []

        if self._tprm_instance.constraint:

            def check_str_constraint(value, constraint: str):
                pattern = re.compile(rf"{constraint}")
                if pattern.match(value):
                    return True
                return np.nan

            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(
                check_str_constraint, constraint=self._tprm_instance.constraint
            )
            not_valid_constraint_indexes = validation_result[
                validation_result.isna()
            ].index.tolist()

            not_valid_constraint_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_valid_constraint_indexes,
                )
            )

            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_constraint_values,
                    error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                    error_indexes=not_valid_constraint_indexes,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _int_column_validation(self):
        errors = []
        warnings = []

        def check_int_column_type(value):
            if value:
                try:
                    if "." in value:
                        int(float(value))
                        return True
                    int(value)
                    return None
                except ValueError:
                    return False

            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_int_column_type)
        not_valid_type_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()

        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )
        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )

        warning_indexes = validation_result[
            validation_result == True  # noqa
        ].index.tolist()
        if warning_indexes:
            warning_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=warning_indexes,
            )
            warnings.extend(
                self._format_warning_structure_for_batch_preview(
                    warning_values=warning_values,
                    warning_indexes=warning_indexes,
                )
            )

        if not_valid_type_indexes:
            self._dataframe_for_validation.drop(
                not_valid_type_indexes, inplace=True
            )

        if self._tprm_instance.constraint:
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME] = (
                validation_result
            )

            bottom, top = self._tprm_instance.constraint.split(":")
            top = int(top)
            bottom = int(bottom)

            def check_int_constraint(value, top: int, bottom: int):
                if pd.isna(value):
                    return True
                return bottom < value < top

            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(check_int_constraint, top=top, bottom=bottom)
            not_valid_constraint_indexes = validation_result[
                validation_result == False  # noqa
            ].index.tolist()
            not_valid_constraint_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_valid_constraint_indexes,
                )
            )
            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_constraint_values,
                    error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                    error_indexes=not_valid_constraint_indexes,
                )
            )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _float_column_validation(self):
        errors = []
        warnings = []

        def check_float_column_type(value):
            if not value:
                return True
            pattern = re.compile(r"^-?\d+(\.\d+)?$")
            if pattern.match(value):
                return float(value)
            return np.nan

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_float_column_type)
        not_valid_type_indexes = validation_result[
            validation_result.isna()
        ].index.tolist()
        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )

        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )

        if not_valid_type_indexes:
            self._dataframe_for_validation.drop(
                not_valid_type_indexes, inplace=True
            )

        if self._tprm_instance.constraint:
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME] = (
                validation_result[validation_result.notna()].values.tolist()
            )

            bottom, top = self._tprm_instance.constraint.split(":")
            top = float(top)
            bottom = float(bottom)

            def check_float_constraint(value, top: float, bottom: float):
                return bottom < value < top

            validation_result = self._dataframe_for_validation[
                self._VALUES_COLUMN_NAME
            ].apply(check_float_constraint, top=top, bottom=bottom)
            not_valid_constraint_indexes = validation_result[
                validation_result == False  # noqa
            ].index.tolist()
            not_valid_constraint_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_valid_constraint_indexes,
                )
            )
            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_constraint_values,
                    error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                    error_indexes=not_valid_constraint_indexes,
                )
            )
        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _date_column_validation(self):
        date_format = "%Y-%m-%d"

        errors = []
        warnings = []

        def check_date_column_type(value, date_format: str):
            if value:
                try:
                    datetime.strptime(value, date_format)
                    return True
                except ValueError:
                    return False
            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_date_column_type, date_format=date_format)
        not_valid_type_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )
        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )
        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _datetime_column_validation(self):
        errors = []
        warnings = []

        def check_datetime_column_type(value):
            if not value:
                return True

            value_str = str(value)
            allowed_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]

            for datetime_format in allowed_formats:
                try:
                    datetime.strptime(value_str, datetime_format)
                    return True
                except ValueError:
                    continue

            return False

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(
            check_datetime_column_type,
        )
        not_valid_type_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )
        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )
        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _bool_column_validation(self):
        allowed_values = {"true", "false"}
        warning_values = {"0", "1"}

        errors = []
        warnings = []

        def check_bool_column_type(value, values_list: set):
            if value:
                if value.lower() in values_list:
                    return True
                return False

            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_bool_column_type, values_list=warning_values)
        warning_values_indexes = validation_result[
            validation_result == True  # noqa
        ].index.tolist()

        if warning_values_indexes:
            warning_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=warning_values_indexes,
            )
            warnings.extend(
                self._format_warning_structure_for_batch_preview(
                    warning_values=warning_values,
                    warning_indexes=warning_values_indexes,
                )
            )

        if warning_values_indexes:
            self._dataframe_for_validation.drop(
                warning_values_indexes, inplace=True
            )

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_bool_column_type, values_list=allowed_values)
        not_valid_type_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name="values",
            indexes_to_search=not_valid_type_indexes,
        )
        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )
        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _mo_link_column_validation(self):
        errors = []
        warnings = []

        requested_object_names = [
            name
            for name in self._dataframe_for_validation[self._VALUES_COLUMN_NAME]
            .unique()
            .tolist()
            if name
        ]

        # Check if values are in format "MO_name:TMO_name"
        mo_names_to_search = []
        tmo_names_to_search = []
        has_tmo_format = False

        for mo_name in requested_object_names:
            if isinstance(mo_name, str) and ":" in mo_name:
                has_tmo_format = True
                parts = mo_name.split(":", 1)
                if len(parts) == 2:
                    mo_names_to_search.append(parts[0].strip())
                    tmo_names_to_search.append(parts[1].strip())
                else:
                    mo_names_to_search.append(mo_name)
            else:
                mo_names_to_search.append(mo_name)

        exists_objects = []
        if has_tmo_format and tmo_names_to_search:
            tmo_id_by_name = {}
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                set(tmo_names_to_search)
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.name.in_(chunk))
                tmo_id_by_name.update(
                    {
                        tmo_name: tmo_id
                        for tmo_id, tmo_name in self._session.exec(stmt).all()
                    }
                )

            for i, mo_name in enumerate(mo_names_to_search):
                if i < len(tmo_names_to_search):
                    tmo_name = tmo_names_to_search[i]
                    tmo_id = tmo_id_by_name.get(tmo_name)
                    if tmo_id:
                        for chunk in get_chunked_values_by_sqlalchemy_limit(
                            [mo_name]
                        ):
                            stmt = select(MO.name).where(
                                MO.name.in_(chunk),
                                MO.tmo_id == tmo_id,
                            )
                            exists_objects.extend(
                                self._session.execute(stmt).scalars().all()
                            )
        else:
            if self._tprm_instance.constraint:
                for chunk in get_chunked_values_by_sqlalchemy_limit(
                    mo_names_to_search
                ):
                    stmt = select(MO.name).where(
                        MO.name.in_(chunk),
                        MO.tmo_id == int(self._tprm_instance.constraint),
                    )
                    exists_objects.extend(
                        self._session.execute(stmt).scalars().all()
                    )
            else:
                for chunk in get_chunked_values_by_sqlalchemy_limit(
                    mo_names_to_search
                ):
                    stmt = select(MO.name).where(MO.name.in_(chunk))
                    exists_objects.extend(
                        self._session.execute(stmt).scalars().all()
                    )

                # Check for duplicate names when no constraint
                series = pd.Series(exists_objects)
                not_concrete_names = (
                    series[series.duplicated()].unique().tolist()
                )
                if not_concrete_names:
                    not_concrete_name_indexes = (
                        self._get_indexes_by_values_in_dataframe(
                            dataframe=self._dataframe_for_validation,
                            column_name_with_values=self._VALUES_COLUMN_NAME,
                            column_name_with_indexes=self._INDEX_COLUMN_NAME,
                            values_to_search=not_concrete_names,
                        )
                    )
                    not_concrete_name_values = (
                        self._get_values_from_dataframe_by_indexes(
                            dataframe=self._dataframe_for_validation,
                            column_name=self._VALUES_COLUMN_NAME,
                            indexes_to_search=not_concrete_name_indexes,
                        )
                    )
                    errors.extend(
                        self._format_error_structure_for_batch_preview(
                            error_values=not_concrete_name_values,
                            error_status=NOT_CONCRETE_NAME,
                            error_indexes=not_concrete_name_indexes,
                        )
                    )

                    mo_names_to_search = [
                        name
                        for name in mo_names_to_search
                        if name not in not_concrete_names
                    ]

        not_exists_values = set(mo_names_to_search).difference(
            set(exists_objects)
        )
        not_exists_values = list(not_exists_values)

        if not_exists_values:
            not_exists_original_names = []
            for clean_name in not_exists_values:
                for original_name in requested_object_names:
                    if isinstance(original_name, str) and ":" in original_name:
                        if original_name.split(":", 1)[0].strip() == clean_name:
                            not_exists_original_names.append(original_name)
                            break
                    elif original_name == clean_name:
                        not_exists_original_names.append(original_name)
                        break

            not_exists_objects_indexes = (
                self._get_indexes_by_values_in_dataframe(
                    dataframe=self._dataframe_for_validation,
                    column_name_with_values=self._VALUES_COLUMN_NAME,
                    column_name_with_indexes=self._INDEX_COLUMN_NAME,
                    values_to_search=not_exists_original_names,
                )
            )
            not_exists_objects_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_exists_objects_indexes,
                )
            )
            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_objects_values,
                    error_status=NOT_EXISTS_OBJECTS,
                    error_indexes=not_exists_objects_indexes,
                )
            )
            if not_exists_objects_indexes:
                self._dataframe_for_validation.drop(
                    not_exists_objects_indexes, inplace=True
                )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _prm_link_column_validation(self):
        errors = []
        warnings = []

        not_empty_values = (
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME]
            .dropna()
            .unique()
            .tolist()
        )
        exists_values = []

        for chunk in get_chunked_values_by_sqlalchemy_limit(not_empty_values):
            stmt = select(PRM.value).where(
                PRM.value.in_(chunk),
                PRM.tprm_id == int(self._tprm_instance.constraint),
            )
            exists_values.extend(self._session.execute(stmt).scalars().all())

        not_exists_values = set(not_empty_values).difference(set(exists_values))

        if not_exists_values:
            not_exists_objects_indexes = (
                self._get_indexes_by_values_in_dataframe(
                    dataframe=self._dataframe_for_validation,
                    column_name_with_values=self._VALUES_COLUMN_NAME,
                    column_name_with_indexes=self._INDEX_COLUMN_NAME,
                    values_to_search=not_exists_values,
                )
            )

            not_exists_objects_values = (
                self._get_values_from_dataframe_by_indexes(
                    dataframe=self._dataframe_for_validation,
                    column_name=self._VALUES_COLUMN_NAME,
                    indexes_to_search=not_exists_objects_indexes,
                )
            )
            errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_objects_values,
                    error_status=NOT_EXISTS_OBJECTS,
                    error_indexes=not_exists_objects_indexes,
                )
            )
            if not_exists_objects_indexes:
                self._dataframe_for_validation.drop(
                    not_exists_objects_indexes, inplace=True
                )

        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _two_way_mo_link_column_pre_validation(self):
        return self._mo_link_column_validation()

    def _enum_column_validation(self):
        errors = []
        warnings = []

        def check_enum_column_values_by_constraint(
            value, constraint: list[Any]
        ):
            if value:
                return value in constraint
            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(
            check_enum_column_values_by_constraint,
            constraint=[
                str(v) for v in literal_eval(self._tprm_instance.constraint)
            ],
        )
        not_valid_constraint_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        not_valid_constraint_values = (
            self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_valid_constraint_indexes,
            )
        )
        errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_constraint_values,
                error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                error_indexes=not_valid_constraint_indexes,
            )
        )
        return ErrorsAndWarnings(errors=errors, warnings=warnings)

    def _validate_tprm_values_by_val_type(self, val_type: str):
        validation_methods_by_val_type: dict[str, func] = {
            "str": self._str_column_validation,
            "int": self._int_column_validation,
            "float": self._float_column_validation,
            "date": self._date_column_validation,
            "datetime": self._datetime_column_validation,
            "bool": self._bool_column_validation,
            "mo_link": self._mo_link_column_validation,
            "prm_link": self._prm_link_column_validation,
            enum_val_type_name: self._enum_column_validation,
            two_way_mo_link_val_type_name: self._two_way_mo_link_column_pre_validation,
        }

        validation_method = validation_methods_by_val_type.get(val_type)

        if validation_method:
            error_and_warnings = validation_method()
            return error_and_warnings

        return ErrorsAndWarnings(errors=[], warnings=[])

    def _required_attribute_validation(self):
        empty_cases = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].isnull() | (
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME] == ""
        )
        empty_values_indexes = self._dataframe_for_validation[
            empty_cases
        ].index.tolist()

        if empty_values_indexes:
            self._errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=[
                        None for _ in range(len(empty_values_indexes))
                    ],
                    error_status=EMPTY_VALUE_IN_REQUIRED,
                    error_indexes=empty_values_indexes,
                )
            )

            self._dataframe_for_validation.drop(
                empty_values_indexes, inplace=True
            )

    def validate(self):
        if self._tprm_instance.required:
            self._required_attribute_validation()

        error_and_warnings = self._validate_tprm_values_by_val_type(
            val_type=self._tprm_instance.val_type
        )

        del self._dataframe_for_validation
        error_and_warnings.errors.extend(self._errors)
        return error_and_warnings


class ObjectAttributesValidation(BatchImportValidator):
    def __init__(
        self,
        session: Session,
        mo_attribute_name: str,
        tmo_instance: TMO,
        values_with_indexes: Series,
    ):
        self._session = session

        self._mo_attribute_name = mo_attribute_name
        self._tmo_instance = tmo_instance

        self._mo_parent_attributes = {"p_id", "parent_name"}
        self._mo_point_attributes = {"point_a_name", "point_b_name"}

        self._INDEX_COLUMN_NAME = "indexes"
        self._VALUES_COLUMN_NAME = "values"
        self._dataframe_for_validation = DataFrame(
            {
                self._INDEX_COLUMN_NAME: values_with_indexes.index,
                self._VALUES_COLUMN_NAME: values_with_indexes.values,
            }
        )

        self._errors: list = []
        self._warnings: list = []

        self._parent_can_be_set = True

    def _check_parent_can_be_set(self):
        if self._mo_attribute_name in self._mo_parent_attributes:
            if self._tmo_instance.p_id is None:
                parent_names = self._dataframe_for_validation[
                    self._VALUES_COLUMN_NAME
                ]
                parent_names_values = parent_names.values.tolist()
                parent_names_indexes = parent_names.index.tolist()
                errors = self._format_error_structure_for_batch_preview(
                    error_values=parent_names_values,
                    error_status=PARENT_CANT_BE_SET,
                    error_indexes=parent_names_indexes,
                )
                self._errors.extend(errors)

                self._parent_can_be_set = False

    def _active_column_validation(self):
        def check_active_values(value: str):
            if value:
                allowed_bool_values = {"true", "false"}
                warning_bool_values = {"0", "1"}

                if value.lower() in allowed_bool_values:
                    return True

                if value.lower() in warning_bool_values:
                    return False

                return None
            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_active_values)

        not_valid_type_indexes = validation_result[
            validation_result.isna()
        ].index.tolist()
        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )

        self._errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_ATTR_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )

        warning_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        warning_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=warning_indexes,
        )

        self._warnings.extend(
            self._format_error_structure_for_batch_preview(
                error_values=warning_values,
                error_status=NOT_VALID_ATTR_VALUE_TYPE,
                error_indexes=warning_indexes,
            )
        )

        return ErrorsAndWarnings(errors=self._errors, warnings=self._warnings)

    def _geometry_column_validation(self):
        def check_pov_values(value: str):
            if value:
                value = value.replace("'", '"')
                try:
                    json_object = json.loads(value)
                    return isinstance(json_object, dict)
                except ValueError:
                    return False
            return None

        validation_result = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(check_pov_values)

        not_valid_type_indexes = validation_result[
            validation_result == False  # noqa
        ].index.tolist()
        not_valid_type_values = self._get_values_from_dataframe_by_indexes(
            dataframe=self._dataframe_for_validation,
            column_name=self._VALUES_COLUMN_NAME,
            indexes_to_search=not_valid_type_indexes,
        )

        self._errors.extend(
            self._format_error_structure_for_batch_preview(
                error_values=not_valid_type_values,
                error_status=NOT_VALID_ATTR_VALUE_TYPE,
                error_indexes=not_valid_type_indexes,
            )
        )

        return ErrorsAndWarnings(errors=self._errors, warnings=self._warnings)

    def _parent_id_column_validation(self):
        if not self._parent_can_be_set:
            return ErrorsAndWarnings([], [])

        def _int_validation(value):
            return value.isdigit() if value else None

        parent_id_validation = self._dataframe_for_validation[
            self._VALUES_COLUMN_NAME
        ].apply(_int_validation)

        not_valid_type_indexes = parent_id_validation[
            parent_id_validation == False  # noqa
        ].index.tolist()

        if not_valid_type_indexes:
            not_valid_type_values = self._get_values_from_dataframe_by_indexes(
                dataframe=self._dataframe_for_validation,
                column_name=self._VALUES_COLUMN_NAME,
                indexes_to_search=not_valid_type_indexes,
            )

            self._errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_valid_type_values,
                    error_status=NOT_VALID_ATTR_VALUE_TYPE,
                    error_indexes=not_valid_type_indexes,
                )
            )
            self._dataframe_for_validation.drop(
                not_valid_type_indexes, inplace=True
            )

        valid_parent_ids = (
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME]
            .dropna()
            .unique()
            .tolist()
        )

        exists_parent_ids = []
        for chunk in get_chunked_values_by_sqlalchemy_limit(valid_parent_ids):
            stmt = select(cast(MO.id, String)).where(
                MO.tmo_id == self._tmo_instance.p_id, MO.id.in_(chunk)
            )
            exists_parent_ids.extend(
                self._session.execute(stmt).scalars().all()
            )

        not_exists_parent_ids = list(
            set(valid_parent_ids).difference(set(exists_parent_ids))
        )
        if not_exists_parent_ids:
            self._errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_parent_ids,
                    error_status=NOT_EXISTS_OBJECTS,
                    error_indexes=self._get_indexes_by_values_in_dataframe(
                        dataframe=self._dataframe_for_validation,
                        column_name_with_indexes=self._INDEX_COLUMN_NAME,
                        values_to_search=not_exists_parent_ids,
                        column_name_with_values=self._VALUES_COLUMN_NAME,
                    ),
                )
            )

        return ErrorsAndWarnings(errors=self._errors, warnings=self._warnings)

    def _parent_name_column_validation(self):
        if not self._parent_can_be_set:
            return ErrorsAndWarnings([], [])

        requested_parent_names = (
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME]
            .dropna()
            .unique()
            .tolist()
        )

        exists_parents = []
        for chunk in get_chunked_values_by_sqlalchemy_limit(
            requested_parent_names
        ):
            stmt = select(MO.name).where(
                MO.tmo_id == self._tmo_instance.p_id, MO.name.in_(chunk)
            )
            exists_parents.extend(self._session.execute(stmt).scalars().all())

        not_exists_parent_names = set(requested_parent_names).difference(
            set(exists_parents)
        )
        not_exists_parent_names = list(not_exists_parent_names)

        if not_exists_parent_names:
            self._errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_parent_names,
                    error_status=NOT_EXISTS_OBJECTS,
                    error_indexes=self._get_indexes_by_values_in_dataframe(
                        dataframe=self._dataframe_for_validation,
                        column_name_with_indexes=self._INDEX_COLUMN_NAME,
                        column_name_with_values=self._VALUES_COLUMN_NAME,
                        values_to_search=not_exists_parent_names,
                    ),
                )
            )

        return ErrorsAndWarnings(errors=self._errors, warnings=self._warnings)

    def _point_name_column_validation(self):
        requested_point_names = (
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME]
            .dropna()
            .unique()
            .tolist()
        )

        point_names_to_search = []
        tmo_names_to_search = []
        exists_points_names = []
        has_tmo_format = False

        for point_name in requested_point_names:
            if isinstance(point_name, str) and ":" in point_name:
                has_tmo_format = True
                parts = point_name.split(":", 1)
                if len(parts) == 2:
                    point_names_to_search.append(parts[0].strip())
                    tmo_names_to_search.append(parts[1].strip())
                else:
                    point_names_to_search.append(point_name)
            else:
                point_names_to_search.append(point_name)

        if has_tmo_format and tmo_names_to_search:
            tmo_id_by_name = {}
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                set(tmo_names_to_search)
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.name.in_(chunk))
                tmo_id_by_name.update(
                    {
                        tmo_name: tmo_id
                        for tmo_id, tmo_name in self._session.exec(stmt).all()
                    }
                )

            for i, point_name in enumerate(point_names_to_search):
                if i < len(tmo_names_to_search):
                    tmo_name = tmo_names_to_search[i]
                    tmo_id = tmo_id_by_name.get(tmo_name)
                    if tmo_id:
                        for chunk in get_chunked_values_by_sqlalchemy_limit(
                            [point_name]
                        ):
                            stmt = select(MO).where(
                                MO.name.in_(chunk),
                                MO.tmo_id == tmo_id,
                            )
                            exists_points: list[MO] = (
                                self._session.execute(stmt).scalars().all()
                            )
                            exists_points_names.extend(
                                [
                                    exists_point.name
                                    for exists_point in exists_points
                                ]
                            )
        else:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                point_names_to_search
            ):
                if self._tmo_instance.points_constraint_by_tmo:
                    stmt = select(MO).where(
                        MO.tmo_id.in_(
                            self._tmo_instance.points_constraint_by_tmo
                        ),
                        MO.name.in_(chunk),
                    )
                else:
                    stmt = select(MO).where(MO.name.in_(chunk))

                exists_points: list[MO] = (
                    self._session.execute(stmt).scalars().all()
                )
                exists_points_names.extend(
                    [exists_point.name for exists_point in exists_points]
                )

                not_exists_objects_status = NOT_EXISTS_OBJECTS
                if self._tmo_instance.points_constraint_by_tmo:
                    not_exists_objects_status = NOT_VALID_VALUE_BY_CONSTRAINT

                    tmo_id_by_mo_names = {}
                    not_concrete_names = []
                    for exists_point in exists_points:
                        if (
                            exists_point.name in tmo_id_by_mo_names
                            and tmo_id_by_mo_names[exists_point.name]
                            != exists_point.tmo_id
                        ):
                            not_concrete_names.append(exists_point.name)

                        else:
                            tmo_id_by_mo_names[exists_point.name] = (
                                exists_point.tmo_id
                            )

                    if not_concrete_names:
                        not_valid_indexes = self._get_indexes_by_values_in_dataframe(
                            dataframe=self._dataframe_for_validation,
                            column_name_with_indexes=self._INDEX_COLUMN_NAME,
                            column_name_with_values=self._VALUES_COLUMN_NAME,
                            values_to_search=not_concrete_names,
                        )
                        self._errors.extend(
                            self._format_error_structure_for_batch_preview(
                                error_values=not_concrete_names,
                                error_status=NOT_CONCRETE_NAME,
                                error_indexes=not_valid_indexes,
                            )
                        )

        not_exists_objects_status = NOT_EXISTS_OBJECTS
        if self._tmo_instance.points_constraint_by_tmo:
            not_exists_objects_status = NOT_VALID_VALUE_BY_CONSTRAINT

        not_exists_points = set(point_names_to_search).difference(
            set(exists_points_names)
        )
        not_exists_points = list(not_exists_points)

        if not_exists_points:
            not_exists_original_names = []
            for clean_name in not_exists_points:
                for original_name in requested_point_names:
                    if isinstance(original_name, str) and ":" in original_name:
                        if original_name.split(":", 1)[0].strip() == clean_name:
                            not_exists_original_names.append(original_name)
                            break
                    elif original_name == clean_name:
                        not_exists_original_names.append(original_name)
                        break

            self._errors.extend(
                self._format_error_structure_for_batch_preview(
                    error_values=not_exists_original_names,
                    error_status=not_exists_objects_status,
                    error_indexes=self._get_indexes_by_values_in_dataframe(
                        dataframe=self._dataframe_for_validation,
                        column_name_with_indexes=self._INDEX_COLUMN_NAME,
                        column_name_with_values=self._VALUES_COLUMN_NAME,
                        values_to_search=not_exists_original_names,
                    ),
                )
            )

        return ErrorsAndWarnings(errors=self._errors, warnings=self._warnings)

    def _point_id_column_validation(self):
        requested_point_ids = (
            self._dataframe_for_validation[self._VALUES_COLUMN_NAME]
            .dropna()
            .unique()
            .tolist()
        )
        requested_point_ids = [
            int(float(point_id)) for point_id in requested_point_ids if point_id
        ]

        exists_points = []
        if self._tmo_instance.points_constraint_by_tmo:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                requested_point_ids
            ):
                stmt = select(MO.id).where(
                    MO.tmo_id.in_(self._tmo_instance.points_constraint_by_tmo),
                    MO.id.in_(chunk),
                )

                exists_points.extend(
                    self._session.execute(stmt).scalars().all()
                )

            not_exists_points = set(requested_point_ids).difference(
                set(exists_points)
            )
            not_exists_points = list(not_exists_points)

            if not_exists_points:
                not_exists_points = list(not_exists_points)
                self._errors.extend(
                    self._format_error_structure_for_batch_preview(
                        error_values=not_exists_points,
                        error_status=NOT_VALID_VALUE_BY_CONSTRAINT,
                        error_indexes=self._get_indexes_by_values_in_dataframe(
                            dataframe=self._dataframe_for_validation,
                            column_name_with_indexes=self._INDEX_COLUMN_NAME,
                            column_name_with_values=self._VALUES_COLUMN_NAME,
                            values_to_search=not_exists_points,
                        ),
                    )
                )

        else:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                requested_point_ids
            ):
                stmt = select(MO.id).where(MO.id.in_(chunk))

                exists_points.extend(
                    self._session.execute(stmt).scalars().all()
                )

            not_exists_points = set(requested_point_ids).difference(
                set(exists_points)
            )
            not_exists_points = list(not_exists_points)

            if not_exists_points:
                not_exists_points = list(not_exists_points)
                self._errors.extend(
                    self._format_error_structure_for_batch_preview(
                        error_values=not_exists_points,
                        error_status=NOT_EXISTS_OBJECTS,
                        error_indexes=self._get_indexes_by_values_in_dataframe(
                            dataframe=self._dataframe_for_validation,
                            column_name_with_indexes=self._INDEX_COLUMN_NAME,
                            column_name_with_values=self._VALUES_COLUMN_NAME,
                            values_to_search=not_exists_points,
                        ),
                    )
                )

        return ErrorsAndWarnings(errors=self._errors, warnings=self._warnings)

    def _validate_values_by_attribute_name(self):
        validation_methods_by_mo_attrs: dict[str, func] = {
            "active": self._active_column_validation,
            "geometry": self._geometry_column_validation,
            "pov": self._geometry_column_validation,
            "p_id": self._parent_id_column_validation,
            "parent_name": self._parent_name_column_validation,
            "point_a_name": self._point_name_column_validation,
            "point_b_name": self._point_name_column_validation,
            "point_a_id": self._point_id_column_validation,
            "point_b_id": self._point_id_column_validation,
        }

        validation_method = validation_methods_by_mo_attrs.get(
            self._mo_attribute_name
        )

        if validation_method:
            error_and_warnings = validation_method()
            return error_and_warnings

        return ErrorsAndWarnings(errors=[], warnings=[])

    def validate(self):
        self._check_parent_can_be_set()

        error_and_warnings = self._validate_values_by_attribute_name()
        del self._dataframe_for_validation
        error_and_warnings.errors.extend(self._errors)
        return error_and_warnings


class MultipleTPRMValuesConverter:
    def __init__(
        self,
        session: Session,
        values_to_convert: list[Any],
        tprm_instance: TPRM,
    ):
        self._session = session
        self._tprm_instance = tprm_instance

        self._VALUES_COLUMN_NAME = "values"
        self._dataframe_for_process = DataFrame(
            {self._VALUES_COLUMN_NAME: values_to_convert}
        )

    def _str_column_converter_multiple(self):
        def convert_str_to_list(multiple_value):
            return (
                literal_eval(multiple_value)
                if pd.notna(multiple_value)
                else None
            )

        values_to_convert = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ]
        converted_values = values_to_convert.apply(
            convert_str_to_list
        ).values.tolist()

        return converted_values

    def _int_column_converter_multiple(self):
        def convert_to_int_values_list(multiple_value):
            if multiple_value:
                if isinstance(multiple_value, list):
                    values = multiple_value
                else:
                    values = literal_eval(multiple_value)
                return [
                    1
                    if isinstance(value, str) and value.lower() == "true"
                    else 0
                    if isinstance(value, str) and value.lower() == "false"
                    else int(value)
                    for value in values
                ]
            return None

        values_to_convert = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ]
        converted_values = [
            convert_to_int_values_list(value) for value in values_to_convert
        ]

        return converted_values

    def _float_column_converter_multiple(self):
        def convert_to_float_values_list(multiple_value):
            if pd.notna(multiple_value):
                return [float(value) for value in literal_eval(multiple_value)]
            return None

        values_to_convert = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ]
        converted_values = values_to_convert.apply(
            convert_to_float_values_list
        ).values.tolist()

        return converted_values

    def _bool_column_converter_multiple(self):
        def convert_to_bool_values_list(value):
            new_value = []
            if value:
                for value in literal_eval(value):
                    if value in {"true", "1"}:
                        new_value.append(True)
                        continue
                    new_value.append(False)

                return new_value

            return None

        values_to_convert = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ]
        converted_values = values_to_convert.apply(
            convert_to_bool_values_list
        ).values.tolist()

        return converted_values

    def _mo_link_column_converter_multiple(self):
        mo_link_values_int_string_view = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ]

        converted_string_values_to_list = [
            literal_eval(value) if value else None
            for value in mo_link_values_int_string_view
        ]

        all_mo_link_names = set()
        mo_names_to_search = []
        tmo_names_to_search = []
        has_tmo_format = False

        for value in converted_string_values_to_list:
            if value:
                for mo_link_name in value:
                    if isinstance(mo_link_name, str) and ":" in mo_link_name:
                        has_tmo_format = True
                        parts = mo_link_name.split(":", 1)
                        if len(parts) == 2:
                            mo_names_to_search.append(parts[0].strip())
                            tmo_names_to_search.append(parts[1].strip())
                        else:
                            mo_names_to_search.append(mo_link_name)
                    else:
                        mo_names_to_search.append(mo_link_name)
                    all_mo_link_names.add(mo_link_name)

        mo_id_by_name = {}

        if has_tmo_format and tmo_names_to_search:
            tmo_id_by_name = {}
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                set(tmo_names_to_search)
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.name.in_(chunk))
                tmo_id_by_name.update(
                    {
                        tmo_name: tmo_id
                        for tmo_id, tmo_name in self._session.exec(stmt).all()
                    }
                )

            for i, mo_name in enumerate(mo_names_to_search):
                if i < len(tmo_names_to_search):
                    tmo_name = tmo_names_to_search[i]
                    tmo_id = tmo_id_by_name.get(tmo_name)
                    if tmo_id:
                        for chunk in get_chunked_values_by_sqlalchemy_limit(
                            [mo_name]
                        ):
                            stmt = select(MO.id, MO.name).where(
                                MO.name.in_(chunk),
                                MO.tmo_id == tmo_id,
                            )
                            mo_id_by_name.update(
                                {
                                    mo_name: mo_id
                                    for mo_id, mo_name in self._session.exec(
                                        stmt
                                    ).all()
                                }
                            )
        else:
            query_filter = (
                MO.tmo_id == int(self._tprm_instance.constraint)
                if self._tprm_instance.constraint
                else True
            )
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                all_mo_link_names
            ):
                stmt = select(MO.id, MO.name).where(
                    MO.name.in_(chunk), query_filter
                )
                mo_id_by_name.update(
                    {
                        mo_name: mo_id
                        for mo_id, mo_name in self._session.exec(stmt).all()
                    }
                )

        converted_values = []
        for value in converted_string_values_to_list:
            if value:
                converted_list = []
                for mo_link_name in value:
                    if isinstance(mo_link_name, str) and ":" in mo_link_name:
                        # Extract MO name from "MO_name:TMO_name" format
                        mo_name = mo_link_name.split(":", 1)[0].strip()
                        mo_id = mo_id_by_name.get(mo_name)
                    else:
                        mo_id = mo_id_by_name.get(mo_link_name)

                    if mo_id is not None:
                        converted_list.append(mo_id)
                converted_values.append(
                    converted_list if converted_list else None
                )
            else:
                converted_values.append(None)

        return converted_values

    def _prm_link_column_converter_multiple(self):
        prm_link_values = self._dataframe_for_process[self._VALUES_COLUMN_NAME]

        converted_string_values_to_list = [
            literal_eval(value) if value else None for value in prm_link_values
        ]

        all_prm_values = set()
        for value in converted_string_values_to_list:
            if value:
                all_prm_values.update(value)

        prm_value_by_id = {}

        query_filter = (
            PRM.tprm_id == int(self._tprm_instance.constraint)
            if self._tprm_instance.constraint
            else True
        )
        for chunk in get_chunked_values_by_sqlalchemy_limit(all_prm_values):
            stmt = select(PRM.id, PRM.value).where(
                PRM.value.in_(chunk), query_filter
            )
            prm_value_by_id.update(
                {
                    value: prm_id
                    for prm_id, value in self._session.exec(stmt).all()
                }
            )

        converted_values = [
            [prm_value_by_id[value] for value in param_value]
            if param_value
            else None
            for param_value in converted_string_values_to_list
        ]

        return converted_values

    def _convert_tprm_values_by_val_type(self, val_type: str):
        convert_values_by_val_type_multiple = {
            "str": self._str_column_converter_multiple,
            "int": self._int_column_converter_multiple,
            "float": self._float_column_converter_multiple,
            "date": self._str_column_converter_multiple,
            "datetime": self._str_column_converter_multiple,
            "bool": self._bool_column_converter_multiple,
            "mo_link": self._mo_link_column_converter_multiple,
            "prm_link": self._prm_link_column_converter_multiple,
            enum_val_type_name: self._str_column_converter_multiple,
            two_way_mo_link_val_type_name: self._mo_link_column_converter_multiple,
        }

        conversion_method = convert_values_by_val_type_multiple.get(val_type)

        if conversion_method:
            converted_values = conversion_method()
            return converted_values

        return self._dataframe_for_process[self._VALUES_COLUMN_NAME].tolist()

    def convert(self):
        converted_values = self._convert_tprm_values_by_val_type(
            self._tprm_instance.val_type
        )

        return converted_values


class SingleTPRMValuesConverter:
    def __init__(
        self,
        session: Session,
        values_to_convert: list[Any],
        tprm_instance: TPRM,
    ):
        self._session = session
        self._tprm_instance = tprm_instance

        self._VALUES_COLUMN_NAME = "values"
        self._dataframe_for_process = DataFrame(
            {self._VALUES_COLUMN_NAME: values_to_convert}
        )

    def _str_column_converter(self):
        converted_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].replace("", None)
        return converted_values.tolist()

    def _int_column_converter(self):
        def int_values_converter(value):
            if pd.notnull(value):
                if "." in value:
                    return int(float(value))
                return int(value)

            return None

        converted_values = (
            self._dataframe_for_process[self._VALUES_COLUMN_NAME]
            .apply(int_values_converter)
            .where(lambda x: x.notna(), None)
            .astype("Int64")
        )
        return converted_values.tolist()

    def _float_column_converter(self):
        def float_values_converter(value):
            return float(value) if pd.notnull(value) else None

        converted_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].apply(float_values_converter)
        return converted_values.tolist()

    def _bool_column_converter(self):
        old_and_new_values = {
            "": None,
            "true": True,
            "1": True,
            "false": False,
            "True": True,
            "False": False,
            "0": False,
        }

        converted_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].replace(old_and_new_values)
        return converted_values.tolist()

    def _mo_link_column_converter(self):
        all_mo_link_names = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].tolist()

        mo_id_by_name = dict()

        mo_names_to_search = []
        tmo_names_to_search = []
        has_tmo_format = False

        for mo_link_name in all_mo_link_names:
            if (
                mo_link_name
                and isinstance(mo_link_name, str)
                and ":" in mo_link_name
            ):
                has_tmo_format = True
                parts = mo_link_name.split(":", 1)
                if len(parts) == 2:
                    mo_names_to_search.append(parts[0].strip())
                    tmo_names_to_search.append(parts[1].strip())
                else:
                    mo_names_to_search.append(mo_link_name)
            elif mo_link_name:
                mo_names_to_search.append(mo_link_name)

        if has_tmo_format and tmo_names_to_search:
            tmo_id_by_name = {}
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                set(tmo_names_to_search)
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.name.in_(chunk))
                tmo_id_by_name.update(
                    {
                        tmo_name: tmo_id
                        for tmo_id, tmo_name in self._session.exec(stmt).all()
                    }
                )

            for i, mo_name in enumerate(mo_names_to_search):
                if i < len(tmo_names_to_search):
                    tmo_name = tmo_names_to_search[i]
                    tmo_id = tmo_id_by_name.get(tmo_name)
                    if tmo_id:
                        for chunk in get_chunked_values_by_sqlalchemy_limit(
                            [mo_name]
                        ):
                            stmt = select(MO.id, MO.name).where(
                                MO.name.in_(chunk),
                                MO.tmo_id == tmo_id,
                            )
                            mo_id_by_name.update(
                                {
                                    mo_name: mo_id
                                    for mo_id, mo_name in self._session.exec(
                                        stmt
                                    ).all()
                                }
                            )
        else:
            if self._tprm_instance.constraint:
                for chunk in get_chunked_values_by_sqlalchemy_limit(
                    all_mo_link_names
                ):
                    stmt = select(MO.id, MO.name).where(
                        MO.name.in_(chunk),
                        MO.tmo_id == int(self._tprm_instance.constraint),
                    )
                    mo_id_by_name.update(
                        {
                            mo_name: mo_id
                            for mo_id, mo_name in self._session.exec(stmt).all()
                        }
                    )
            else:
                for chunk in get_chunked_values_by_sqlalchemy_limit(
                    all_mo_link_names
                ):
                    stmt = select(MO.id, MO.name).where(MO.name.in_(chunk))
                    mo_id_by_name.update(
                        {
                            mo_name: mo_id
                            for mo_id, mo_name in self._session.exec(stmt).all()
                        }
                    )

        converted_values = []
        for mo_link_name in all_mo_link_names:
            if mo_link_name:
                if isinstance(mo_link_name, str) and ":" in mo_link_name:
                    mo_name = mo_link_name.split(":", 1)[0].strip()
                    new_value = mo_id_by_name.get(mo_name)
                else:
                    new_value = mo_id_by_name.get(mo_link_name)

                if new_value is not None:
                    converted_values.append(int(new_value))
                else:
                    converted_values.append(None)
            else:
                converted_values.append(None)

        self._dataframe_for_process[self._VALUES_COLUMN_NAME] = converted_values
        converted_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].astype("Int64")
        return converted_values

    def _convert_tprm_values_by_val_type(self, val_type: str):
        convert_values_by_val_type = {
            "str": self._str_column_converter,
            "int": self._int_column_converter,
            "float": self._float_column_converter,
            "date": self._str_column_converter,
            "datetime": self._str_column_converter,
            "bool": self._bool_column_converter,
            "mo_link": self._mo_link_column_converter,
            "prm_link": self._int_column_converter,
            enum_val_type_name: self._str_column_converter,
            two_way_mo_link_val_type_name: self._int_column_converter,
        }
        conversion_method = convert_values_by_val_type.get(val_type)

        if conversion_method:
            converted_values = conversion_method()
            return converted_values

        return self._dataframe_for_process[self._VALUES_COLUMN_NAME].tolist()

    def convert(self):
        self._dataframe_for_process[self._VALUES_COLUMN_NAME] = (
            self._dataframe_for_process[self._VALUES_COLUMN_NAME].replace(
                "", None
            )
        )

        converted_values = self._convert_tprm_values_by_val_type(
            self._tprm_instance.val_type
        )

        return converted_values


class ObjectAttributeValuesConverter:
    def __init__(
        self,
        session: Session,
        values_to_convert: list[Any],
        attribute_name: str,
        object_type_instance: TMO,
    ):
        self._session = session

        self._attribute_name = attribute_name

        self._object_type_instance = object_type_instance

        self._VALUES_COLUMN_NAME = "values"
        self._dataframe_for_process = DataFrame(
            {self._VALUES_COLUMN_NAME: values_to_convert}
        )

    def _active_column_converter(self):
        old_and_new_values = {
            "": None,
            "true": True,
            "1": True,
            "false": False,
            "0": False,
        }

        def convert_to_lower_case_all_values(value):
            return str(value).lower()

        lower_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].apply(convert_to_lower_case_all_values)
        self._dataframe_for_process[self._VALUES_COLUMN_NAME] = (
            lower_values.values.tolist()
        )

        converted_values = (
            self._dataframe_for_process[self._VALUES_COLUMN_NAME]
            .replace(old_and_new_values)
            .tolist()
        )
        return converted_values

    @staticmethod
    def _convert_value_to_json(value: str):
        if value:
            value = value.replace("'", '"')
            return dict(json.loads(value))

        return None

    def _geometry_column_converter(self):
        def safe_convert_value_to_json(value):
            try:
                return self._convert_value_to_json(value)
            except json.JSONDecodeError:
                return value
            except Exception:
                return value

        converted_values = (
            self._dataframe_for_process[self._VALUES_COLUMN_NAME]
            .apply(safe_convert_value_to_json)
            .values.tolist()
        )

        return converted_values

    def _p_id_column_converter(self):
        def int_values_converter(value):
            if pd.notnull(value):
                return int(value)

            return None

        converted_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].apply(int_values_converter)
        return converted_values.tolist()

    def _parent_name_column_converter(self):
        parent_names = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].tolist()

        parent_name_by_id = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(parent_names):
            stmt = select(MO.id, MO.name).where(
                MO.name.in_(chunk), MO.tmo_id == self._object_type_instance.p_id
            )
            parent_name_by_id.update(
                {
                    p_name: p_id
                    for p_id, p_name in self._session.exec(stmt).all()
                }
            )

        converted_values = []
        for parent_name in parent_names:
            if parent_name:
                converted_value = parent_name_by_id.get(parent_name)
            else:
                converted_value = None

            converted_values.append(converted_value)
        return converted_values

    def _points_name_column_converter(self):
        point_names = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].tolist()

        point_names_to_search = []
        tmo_names_to_search = []
        has_tmo_format = False

        for point_name in point_names:
            if point_name and isinstance(point_name, str) and ":" in point_name:
                has_tmo_format = True
                parts = point_name.split(":", 1)
                if len(parts) == 2:
                    point_names_to_search.append(parts[0].strip())
                    tmo_names_to_search.append(parts[1].strip())
                else:
                    point_names_to_search.append(point_name)
            elif point_name:
                point_names_to_search.append(point_name)

        point_name_by_id = {}
        if has_tmo_format and tmo_names_to_search:
            tmo_id_by_name = {}
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                set(tmo_names_to_search)
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.name.in_(chunk))
                tmo_id_by_name.update(
                    {
                        tmo_name: tmo_id
                        for tmo_id, tmo_name in self._session.exec(stmt).all()
                    }
                )

            for i, point_name in enumerate(point_names_to_search):
                if i < len(tmo_names_to_search):
                    tmo_name = tmo_names_to_search[i]
                    tmo_id = tmo_id_by_name.get(tmo_name)
                    if tmo_id:
                        for chunk in get_chunked_values_by_sqlalchemy_limit(
                            [point_name]
                        ):
                            stmt = select(MO.id, MO.name).where(
                                MO.name.in_(chunk),
                                MO.tmo_id == tmo_id,
                            )
                            point_name_by_id.update(
                                {
                                    point_name: point_id
                                    for point_id, point_name in self._session.exec(
                                        stmt
                                    ).all()
                                }
                            )
        else:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                point_names_to_search
            ):
                if self._object_type_instance.points_constraint_by_tmo:
                    stmt = select(MO.id, MO.name).where(
                        MO.name.in_(chunk),
                        MO.tmo_id.in_(
                            self._object_type_instance.points_constraint_by_tmo
                        ),
                    )
                else:
                    stmt = select(MO.id, MO.name).where(MO.name.in_(chunk))

                point_name_by_id.update(
                    {
                        point_name: point_id
                        for point_id, point_name in self._session.exec(
                            stmt
                        ).all()
                    }
                )

        converted_values = []
        for point_name in point_names:
            if point_name:
                if isinstance(point_name, str) and ":" in point_name:
                    actual_point_name = point_name.split(":", 1)[0].strip()
                    converted_value = point_name_by_id.get(actual_point_name)
                else:
                    converted_value = point_name_by_id.get(point_name)
            else:
                converted_value = None

            converted_values.append(converted_value)
        return converted_values

    def _points_id_column_converter(self):
        def int_values_converter(value):
            if value:
                return int(float(value))

            return None

        converted_values = self._dataframe_for_process[
            self._VALUES_COLUMN_NAME
        ].apply(int_values_converter)
        return converted_values.tolist()

    def _convert_tprm_values_by_attr_name(self, attribute_name: str):
        convert_values_of_mo_attributes = {
            "active": self._active_column_converter,
            "geometry": self._geometry_column_converter,
            "pov": self._geometry_column_converter,
            "p_id": self._p_id_column_converter,
            "parent_name": self._parent_name_column_converter,
            "point_a_name": self._points_name_column_converter,
            "point_b_name": self._points_name_column_converter,
            "point_a_id": self._points_id_column_converter,
            "point_b_id": self._points_id_column_converter,
        }
        conversion_method = convert_values_of_mo_attributes.get(attribute_name)

        if conversion_method:
            converted_values = conversion_method()
            return converted_values

        return self._dataframe_for_process[self._VALUES_COLUMN_NAME].tolist()

    def convert(self):
        converted_values = self._convert_tprm_values_by_attr_name(
            self._attribute_name
        )

        return converted_values


class BatchImportCreator(BatchImportValidator):
    def __init__(
        self,
        file: bytes,
        session: Session,
        object_type_id: int,
        column_name_mapping: dict,
        delimiter: str,
        check: bool = False,
        background_tasks: BackgroundTasks = None,
        file_content_type: str = "text/csv",
        force: bool = False,
    ):
        raise_errors = ErrorProcessor.RAISE

        if force:
            raise_errors = ErrorProcessor.COLLECT

        super().__init__(
            file=file,
            session=session,
            object_type_id=object_type_id,
            column_name_mapping=column_name_mapping,
            delimiter=delimiter,
            raise_errors=raise_errors,
            file_content_type=file_content_type,
        )

        self._check_data = check
        self._background_tasks = background_tasks

    @staticmethod
    def slice_dataframe(df: pd.DataFrame, size: int):
        for start in range(0, len(df), size):
            yield df.iloc[start : start + size]

    @staticmethod
    def slice_iter_object(data, size: int):
        for start in range(0, len(data), size):
            yield data[start : start + size]

    def _create_object_and_parameters(self) -> None:
        mo_id_mapping: List[dict[str, Any]] = []

        for df_slice in self.slice_dataframe(
            df=self._create_object_parameters_and_attributes, size=20_000
        ):
            batch_created = []

            for _, row_with_values in df_slice.iterrows():
                row_dict: dict[str, Any] = {
                    column: value
                    for column, value in row_with_values.to_dict().items()
                    if value is not None
                }

                mo = MO(**row_dict, tmo_id=self._object_type_instance.id)

                if "parent_name" in row_dict:
                    mo.p_id = int(row_dict["parent_name"])
                if "point_a_name" in row_dict:
                    mo.point_a_id = row_dict["point_a_name"]
                if "point_b_name" in row_dict:
                    mo.point_b_id = row_dict["point_b_name"]

                if self._object_type_instance.label:
                    mo.label = row_dict.get("label")

                if self._object_type_instance.primary:
                    mo.name = row_dict.get(COMBINED_NAMES_COLUMN)

                self._session.add(mo)
                mo_id_mapping.append({"mo": mo, "row_dict": row_dict})
                batch_created.append(mo)

            self._session.flush()
            for object_instance in batch_created:
                self._session.expunge(object_instance)

        for instances in self.slice_iter_object(data=mo_id_mapping, size=1_000):
            processed_parameters = []
            for _, instance in enumerate(instances):
                mo = instance["mo"]
                row_dict = instance["row_dict"]
                if not self._object_type_instance.primary:
                    mo.name = str(mo.id)
                    self._session.add(mo)

                new_tprm_parameters = {
                    column_name: value
                    for column_name, value in row_dict.items()
                    if column_name.isdigit()
                }
                for column_name, new_value in new_tprm_parameters.items():
                    current_tprm = self._tprm_instance_by_id[int(column_name)]
                    value = (
                        pickle.dumps(new_value).hex()
                        if current_tprm.multiple
                        else new_value
                    )
                    if pd.notna(value):
                        prm_dict = {
                            "tprm_id": current_tprm.id,
                            "mo_id": mo.id,
                            "value": str(value),
                        }
                        processed_parameters.append(prm_dict)

            self._session.bulk_insert_mappings(PRM, processed_parameters)

    def _update_object_attributes(self):
        object_ids_for_update = (
            self._updated_object_attributes[MO_ID_COLUMN].unique().tolist()
        )

        object_instance_by_id: dict[int, MO] = dict()
        for chunk in get_chunked_values_by_sqlalchemy_limit(
            object_ids_for_update
        ):
            stmt = select(MO).where(MO.id.in_([int(mo_id) for mo_id in chunk]))
            object_instance_by_id.update(
                {mo.id: mo for mo in self._session.execute(stmt).scalars()}
            )

        for _, row in self._updated_object_attributes.iterrows():
            object_instance = object_instance_by_id[row[MO_ID_COLUMN]]
            value_for_update = row["new_value"]
            attribute = row["attr_name"]

            if attribute == "parent_name":
                object_instance.p_id = int(value_for_update)

            elif attribute == "point_a_name":
                object_instance.point_a_id = value_for_update

            elif attribute == "point_b_name":
                object_instance.point_b_id = value_for_update

            elif self._object_type_instance.label:
                object_instance.label = row["label"]

            else:
                setattr(object_instance, attribute, value_for_update)

        for updated_object in object_instance_by_id.values():
            self._object_ids_to_update.add(updated_object.id)
            self._session.add(updated_object)

        self._session.flush()

    def _update_parameters(self):
        self._updated_parameters = self._updated_parameters.convert_dtypes()
        updated_prm_ids = (
            self._updated_parameters["prm_id"]
            .astype("float")
            .astype(int)
            .unique()
            .tolist()
        )
        self._updated_parameters["prm_id"] = updated_prm_ids

        parameter_instance_by_id: dict[int, PRM] = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(updated_prm_ids):
            stmt = select(PRM).where(PRM.id.in_(chunk))
            parameter_instance_by_id.update(
                {prm.id: prm for prm in self._session.execute(stmt).scalars()}
            )

        for _, row in self._updated_parameters.iterrows():
            prm_id = row["prm_id"]
            new_value = row["new_value"]
            parameter_instance = parameter_instance_by_id[int(prm_id)]
            parameter_instance.version += 1

            if isinstance(new_value, list):
                new_value = pickle.dumps(new_value).hex()

            parameter_instance.value = str(new_value)
            self._session.add(parameter_instance)
            self._object_ids_to_update.add(parameter_instance.mo_id)

        self._session.flush()

    def _create_parameters(self):
        for _, row in self._created_mo_prms.iterrows():
            new_value = row["value"]
            if isinstance(new_value, list):
                new_value = pickle.dumps(new_value).hex()

            new_prm = PRM(
                version=1,
                mo_id=row[MO_ID_COLUMN],
                tprm_id=row["tprm_id"],
                value=str(new_value),
            )
            self._session.add(new_prm)
            self._object_ids_to_update.add(row[MO_ID_COLUMN])

        self._session.flush()

    def _create_attributes(self):
        object_ids_for_update = (
            self._created_attributes[MO_ID_COLUMN].unique().tolist()
        )

        object_instance_by_id: dict[int, MO] = dict()
        for chunk in get_chunked_values_by_sqlalchemy_limit(
            object_ids_for_update
        ):
            chunk = [int(obj_id) for obj_id in chunk]
            stmt = select(MO).where(MO.id.in_(chunk))
            object_instance_by_id.update(
                {mo.id: mo for mo in self._session.execute(stmt).scalars()}
            )

        for _, row in self._created_attributes.iterrows():
            attribute_name = row["attr_name"]
            new_value = row["value"]
            object_id = row[MO_ID_COLUMN]
            object_instance = object_instance_by_id[object_id]

            if attribute_name == "parent_name":
                object_instance.p_id = int(new_value)

            elif attribute_name == "point_a_name":
                object_instance.point_a_id = new_value

            elif attribute_name == "point_b_name":
                object_instance.point_b_id = new_value

            elif self._object_type_instance.label:
                object_instance.label = row["label"]

            else:
                setattr(object_instance, attribute_name, new_value)

        for updated_object in object_instance_by_id.values():
            self._object_ids_to_update.add(updated_object.id)
            self._session.add(updated_object)

        self._session.flush()

    def _delete_parameters(self):
        object_ids_for_update = (
            self._deleted_object_values[MO_ID_COLUMN]
            .unique()
            .astype(int)
            .tolist()
        )

        object_instance_by_id: dict[int, MO] = dict()
        for chunk in get_chunked_values_by_sqlalchemy_limit(
            object_ids_for_update
        ):
            stmt = select(MO).where(MO.id.in_(chunk))
            object_instance_by_id.update(
                {mo.id: mo for mo in self._session.execute(stmt).scalars()}
            )

        updated_objects_by_id = {}
        objects_to_update = set()

        for _, row in self._deleted_object_values.iterrows():
            attribute = row["attr_name"]
            updated_object_id = row[MO_ID_COLUMN]
            object_instance = object_instance_by_id[updated_object_id]

            if attribute.isdigit():
                stmt = select(PRM).where(
                    PRM.tprm_id == int(attribute),
                    PRM.mo_id == updated_object_id,
                )
                parameter_for_delete = self._session.execute(stmt).scalar()

                self._session.delete(parameter_for_delete)
                objects_to_update.add(updated_object_id)
                self._session.flush()
            else:
                if "parent_name" in row:
                    object_instance.p_id = sql_null()

                elif "point_a_name" in row:
                    object_instance.point_a_id = sql_null()

                elif "point_b_name" in row:
                    object_instance.point_b_id = sql_null()

                elif self._object_type_instance.label:
                    object_instance.label = sql_null()
                else:
                    setattr(object_instance, attribute, sql_null())

                updated_objects_by_id[object_instance.id] = object_instance
            self._object_ids_to_update.add(object_instance.id)

        for updated_object in object_instance_by_id.values():
            self._session.add(updated_object)
            self._session.flush()

    def _session_commit_on_background(self):
        print("before commit", datetime.now())
        self._session.commit()
        print("after commit", datetime.now())

    def _create_data_on_background(self):
        if self._create_object_parameters_and_attributes.any().any():
            self._background_tasks.add_task(self._create_object_and_parameters)

        if self._updated_object_attributes.any().any():
            self._background_tasks.add_task(self._update_object_attributes)

        if self._updated_parameters.any().any():
            self._background_tasks.add_task(self._update_parameters)

        if self._created_mo_prms.any().any():
            self._background_tasks.add_task(self._create_parameters)

        if self._created_attributes.any().any():
            self._background_tasks.add_task(self._create_attributes)

        if self._deleted_object_values.any().any():
            self._background_tasks.add_task(self._delete_parameters)

        self._background_tasks.add_task(self._add_versions_for_updated_objects)

        self._background_tasks.add_task(self._session_commit_on_background)

    def _add_versions_for_updated_objects(self):
        object_instances_to_update: list[MO] = []
        for chunk in get_chunked_values_by_sqlalchemy_limit(
            self._object_ids_to_update
        ):
            query = select(MO).where(MO.id.in_(chunk))
            object_instances_to_update.extend(
                [
                    object_instance
                    for object_instance in self._session.execute(query)
                    .scalars()
                    .all()
                ]
            )

        for object_instance in object_instances_to_update:
            object_instance.modification_date = datetime.now(timezone.utc)
            object_instance.version = object_instance.version + 1
            self._session.add(object_instance)

        self._session.flush()

    def _create_data(self):
        if self._create_object_parameters_and_attributes.any().any():
            self._create_object_and_parameters()

        if self._updated_object_attributes.any().any():
            self._update_object_attributes()

        if self._updated_parameters.any().any():
            self._update_parameters()

        if self._created_mo_prms.any().any():
            self._create_parameters()

        if self._created_attributes.any().any():
            self._create_attributes()

        if self._deleted_object_values.any().any():
            self._delete_parameters()

        self._add_versions_for_updated_objects()
        self._session_commit_on_background()

    def execute(self):
        self.validate_file_data()

        if self._check_data:
            return self._get_data_statistic()

        self._object_ids_to_update = set()
        if self._background_tasks is not None:
            self._create_data_on_background()

        else:
            self._create_data()

        return {
            "status": "ok",
            "detail": "File is valid. Objects will be created soon",
        }


class BatchExportProcessor:
    def __init__(
        self,
        object_type_id: int,
        request: dict,
        file_type: str,
        session: Session = Depends(get_session),
        delimiter: str = Query(default=";", max_length=1, min_length=1),
        obj_ids: Union[List[int], None] = Query(default=None),
        prm_type_ids: Union[List[int], None] = Query(default=None),
        replace_ids_by_names: bool = Query(default=False),
        with_full_attributes: bool = Query(default=False),
    ):
        self._object_type_id = object_type_id
        self._request_data = request
        self._file_type = file_type
        self._session = session
        self._delimiter = delimiter
        self._obj_ids = obj_ids
        self._prm_type_ids = prm_type_ids
        self._replace_ids_by_names = replace_ids_by_names
        self._with_full_attributes = with_full_attributes

        self._output = io.BytesIO()
        self._workbook = xlsxwriter.Workbook(self._output)

        self._cache_column_name_type = {}
        self._main_df = pd.DataFrame()
        self._groups_and_tprm_count = dict()
        self._mo_where_condition = [MO.active == True]  # noqa

    def check(self):
        if self._session.get(TMO, self._object_type_id):
            return

        raise TMONotExists(
            status_code=422,
            detail=f"Object type with id {self._object_type_id} does not exists",
        )

    def _get_query_params(self):
        # get mo_ids which match filter conditions
        query_params = self._request_data["query_params"]
        tprm_cleaner = TPRMFilterCleaner(
            session=self._session,
            query_params=query_params,
            object_type_id=self._object_type_id,
        )

        if tprm_cleaner.check_filter_data_in_query_params():
            self._obj_ids = (
                tprm_cleaner.get_mo_ids_which_match_clean_filter_conditions(
                    obj_ids=self._obj_ids
                )
            )

    def _get_parameter_type_to_filter(self):
        # Adding mo parameters into df
        prm_type_where_condition = []
        if self._prm_type_ids:
            prm_type_where_condition.append(TPRM.id.in_(self._prm_type_ids))

        stmt = (
            select(TPRM)
            .where(
                TPRM.tmo_id == self._object_type_id, *prm_type_where_condition
            )
            .order_by(TPRM.group)
        )

        self._parameter_types_to_fill_data = self._session.exec(stmt).all()

        return self._parameter_types_to_fill_data

    def _get_object_id_to_filter(self):
        if self._obj_ids:
            self._mo_where_condition.append(MO.id.in_(self._obj_ids))

    def _count_tprm_by_groups(self, tprm_group_name):
        if self._groups_and_tprm_count.get(tprm_group_name, None) is None:
            self._groups_and_tprm_count[tprm_group_name] = 0

    def _get_parameters_by_parameter_type_for_column(
        self, parameter_type_id: int
    ):
        stmt = select(PRM.mo_id.label("mo_id"), PRM.value.label("value")).where(
            PRM.tprm_id == parameter_type_id
        )

        aliased_table = aliased(stmt.subquery())

        stmt = (
            select(MO.id.label("mo_id"), aliased_table)
            .outerjoin(aliased_table, MO.id == aliased_table.c.mo_id)
            .where(MO.tmo_id == self._object_type_id, *self._mo_where_condition)
            .order_by(MO.id)
        )

        prm_values = self._session.execute(stmt).all()
        return prm_values

    def _fill_column_data(self, param_type_instance: TPRM, prm_values):
        column_data = []
        convert_by_val_type = value_convertation_by_val_type[
            param_type_instance.val_type
        ]

        if param_type_instance.multiple:
            for prm_data in prm_values:
                if prm_data.value is not None:
                    value = decode_multiple_value(prm_data.value)
                    column_data.append(str(value))
                else:
                    column_data.append("")
        else:
            for prm_data in prm_values:
                if prm_data.value is not None:
                    column_data.append(convert_by_val_type(prm_data.value))
                else:
                    column_data.append("")

        return column_data

    def _convert_values_to_names(
        self, parameter_type_instance: TPRM, column_data: list[str]
    ):
        # for TPRMs with val_type 'mo_link' we store ids in DB,
        # so we need to update response from giving ids to names of linked objects
        if (
            self._replace_ids_by_names
            and parameter_type_instance.val_type == "mo_link"
        ):
            chunk_limit_len = 5000
            rows_chunks = copy.copy(column_data)

            column_data = []

            # we decouple all rows by chunk_limit, because after than it will be easier to decouple ids for DB queries
            for row_index in range(0, len(rows_chunks), chunk_limit_len):
                not_formatted_linked_mo_ids = rows_chunks[
                    row_index : row_index + chunk_limit_len
                ]

                # ths way in chunk we store data like ['[1,2,3]', '[4,5,6]', '', '[7,8,9]']
                # so we need to convert it to ordinary list and get mo_ids from this chunk
                # in future we will get names by these objects and will replace ids by names
                if parameter_type_instance.multiple:
                    mo_ids = set()
                    for linked_mo_id in not_formatted_linked_mo_ids:
                        if linked_mo_id:
                            formatted_mo_ids = json.loads(linked_mo_id)
                            for mo_id in formatted_mo_ids:
                                mo_ids.add(mo_id)

                # this way in chunk we store data like [1,2,'',4,'',6]
                else:
                    mo_ids = {
                        mo_id for mo_id in not_formatted_linked_mo_ids if mo_id
                    }

                mo_ids = list(mo_ids)
                # to get mo_names by ids we need to write query with IN operator.
                # IN operator has limit of list -- 32766 objects,
                # so we get chunk of 32766 objects to get names for them
                mo_ids_chunks = [
                    mo_ids[index : index + SQLALCHEMY_LIMIT]
                    for index in range(0, len(mo_ids), SQLALCHEMY_LIMIT)
                ]

                # here we store mo id, name and tmo_name pairs
                mo_names_and_ids = {}
                tmo_names_by_mo_id = {}
                for mo_ids_chunk in mo_ids_chunks:
                    stmt = select(MO.id, MO.name, MO.tmo_id).where(
                        MO.id.in_(mo_ids_chunk)
                    )
                    data = self._session.exec(stmt).all()
                    mo_names_and_ids.update(
                        {mo_id: mo_name for mo_id, mo_name, tmo_id in data}
                    )
                    tmo_names_by_mo_id.update(
                        {mo_id: tmo_id for mo_id, mo_name, tmo_id in data}
                    )

                tmo_ids = set(tmo_names_by_mo_id.values())
                tmo_names_by_id = {}
                if tmo_ids:
                    for tmo_ids_chunk in [
                        list(tmo_ids)[index : index + SQLALCHEMY_LIMIT]
                        for index in range(0, len(tmo_ids), SQLALCHEMY_LIMIT)
                    ]:
                        stmt = select(TMO.id, TMO.name).where(
                            TMO.id.in_(tmo_ids_chunk)
                        )
                        tmo_data = self._session.exec(stmt).all()
                        tmo_names_by_id.update(
                            {tmo_id: tmo_name for tmo_id, tmo_name in tmo_data}
                        )

                # and now we need to replace ids by names with TMO name prefix
                # here we can store 3 types of data: '', '[1,2,3]' and 1
                # if row is empty -- we need just add it
                # if it multiply and stores in json format we need to convert it and replace ids by names
                # if it just id of some object -- we can replace it by name
                for linked_mo_id in not_formatted_linked_mo_ids:
                    if linked_mo_id:
                        if parameter_type_instance.multiple:
                            formatted_mo_ids = json.loads(linked_mo_id)
                            converted_names_from_ids = []
                            for mo_id in formatted_mo_ids:
                                mo_name = mo_names_and_ids[mo_id]
                                tmo_id = tmo_names_by_mo_id[mo_id]
                                tmo_name = tmo_names_by_id[tmo_id]
                                converted_names_from_ids.append(
                                    f"{mo_name}:{tmo_name}"
                                )
                            column_data.append(
                                json.dumps(converted_names_from_ids)
                            )
                        else:
                            mo_name = mo_names_and_ids[linked_mo_id]
                            tmo_id = tmo_names_by_mo_id[linked_mo_id]
                            tmo_name = tmo_names_by_id[tmo_id]
                            column_data.append(f"{mo_name}:{tmo_name}")

                    else:
                        column_data.append(linked_mo_id)

        return column_data

    def _fill_parent_name_column(self):
        aliased_table = aliased(select(MO).subquery())
        stmt = (
            select(
                MO.id.label("mo_id"), aliased_table.c.name.label("parent_name")
            )
            .outerjoin(aliased_table, MO.p_id == aliased_table.c.id)
            .where(MO.tmo_id == self._object_type_id, *self._mo_where_condition)
            .order_by(MO.id)
        )

        res = self._session.execute(stmt).all()
        temporary_df = pd.DataFrame(data=res, columns=["mo_id", "parent_name"])
        self._main_df["parent_name"] = temporary_df["parent_name"]

    def _fill_points_column(self):
        aliased_table = aliased(select(MO).subquery())
        stmt = (
            select(
                MO.id.label("mo_id"),
                aliased_table.c.name.label("point_a_name"),
                aliased_table.c.tmo_id.label("point_a_tmo_id"),
            )
            .outerjoin(aliased_table, MO.point_a_id == aliased_table.c.id)
            .where(MO.tmo_id == self._object_type_id, *self._mo_where_condition)
            .order_by(MO.id)
        )

        res = self._session.execute(stmt).all()
        temporary_df = pd.DataFrame(
            data=res, columns=["mo_id", "point_a_name", "point_a_tmo_id"]
        )

        point_a_tmo_ids = temporary_df["point_a_tmo_id"].dropna().unique()
        tmo_names_by_id = {}
        if len(point_a_tmo_ids) > 0:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                point_a_tmo_ids
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.id.in_(chunk))
                tmo_data = self._session.exec(stmt).all()
                tmo_names_by_id.update(
                    {tmo_id: tmo_name for tmo_id, tmo_name in tmo_data}
                )

        formatted_point_a_names = []
        for _, row in temporary_df.iterrows():
            if pd.notna(row["point_a_name"]) and pd.notna(
                row["point_a_tmo_id"]
            ):
                point_a_name = row["point_a_name"]
                tmo_id = row["point_a_tmo_id"]
                tmo_name = tmo_names_by_id.get(tmo_id, "")
                formatted_point_a_names.append(f"{point_a_name}:{tmo_name}")
            else:
                formatted_point_a_names.append(row["point_a_name"])

        self._main_df["point_a_name"] = formatted_point_a_names

        aliased_table = aliased(select(MO).subquery())
        stmt = (
            select(
                MO.id.label("mo_id"),
                aliased_table.c.name.label("point_b_name"),
                aliased_table.c.tmo_id.label("point_b_tmo_id"),
            )
            .outerjoin(aliased_table, MO.point_b_id == aliased_table.c.id)
            .where(MO.tmo_id == self._object_type_id, *self._mo_where_condition)
            .order_by(MO.id)
        )

        res = self._session.execute(stmt).all()
        temporary_df = pd.DataFrame(
            data=res, columns=["mo_id", "point_b_name", "point_b_tmo_id"]
        )

        point_b_tmo_ids = temporary_df["point_b_tmo_id"].dropna().unique()
        tmo_names_by_id = {}
        if len(point_b_tmo_ids) > 0:
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                point_b_tmo_ids
            ):
                stmt = select(TMO.id, TMO.name).where(TMO.id.in_(chunk))
                tmo_data = self._session.exec(stmt).all()
                tmo_names_by_id.update(
                    {tmo_id: tmo_name for tmo_id, tmo_name in tmo_data}
                )

        formatted_point_b_names = []
        for _, row in temporary_df.iterrows():
            if pd.notna(row["point_b_name"]) and pd.notna(
                row["point_b_tmo_id"]
            ):
                point_b_name = row["point_b_name"]
                tmo_id = row["point_b_tmo_id"]
                tmo_name = tmo_names_by_id.get(tmo_id, "")
                formatted_point_b_names.append(f"{point_b_name}:{tmo_name}")
            else:
                formatted_point_b_names.append(row["point_b_name"])

        self._main_df["point_b_name"] = formatted_point_b_names

    def _fill_all_object_attributes_column(self):
        if self._with_full_attributes:
            stmt = (
                select(MO)
                .where(
                    MO.tmo_id == self._object_type_id, *self._mo_where_condition
                )
                .order_by(MO.id)
            )

            res = self._session.execute(stmt).all()

            temporary_df = pd.DataFrame(
                [
                    {
                        k: v
                        for k, v in row[0].__dict__.items()
                        if k
                        not in [
                            "_sa_instance_state",
                            "p_id",
                            "point_a_id",
                            "point_b_id",
                        ]
                    }
                    for row in res
                ]
            )

            for col in temporary_df.columns:
                self._main_df[col] = temporary_df[col]

    def _save_csv_file(self):
        if self._file_type == ExportFileTypes.csv.value:
            self._main_df.to_csv(
                path_or_buf=self._output, index=False, sep=self._delimiter
            )
            self._output.seek(0)

    def _save_xlsx_file(self):
        if self._file_type != ExportFileTypes.csv.value:
            start_row_for_df = 0

            cell_format = self._workbook.add_format(
                {
                    "bold": True,
                    "font_color": "black",
                    "align": "center",
                    "valign": "center",
                    "border": 1,
                    "border_color": "black",
                }
            )
            worksheet = self._workbook.add_worksheet("sheet1")

            # Add group headers
            if start_row_for_df == 1:
                first_column = 0
                for (
                    group_name,
                    column_count,
                ) in self._groups_and_tprm_count.items():
                    last_column = first_column + column_count - 1
                    if column_count == 1:
                        worksheet.write(0, last_column, group_name, cell_format)
                    else:
                        worksheet.merge_range(
                            first_row=0,
                            first_col=first_column,
                            last_row=0,
                            last_col=last_column,
                            data=group_name,
                            cell_format=cell_format,
                        )
                    first_column = last_column + 1
            column_number = 0
            row_number = start_row_for_df
            for column in self._main_df:
                worksheet.write(row_number, column_number, column, cell_format)
                row_number += 1

                cell_format = None
                column_type = self._cache_column_name_type.get(column)
                if column_type == "bool":
                    cell_format = self._workbook.add_format(
                        {"num_format": "BOOLEAN"}
                    )
                data_list = [
                    str(val) if val is not None else ""
                    for val in self._main_df[column].values
                ]

                worksheet.write_column(
                    row_number,
                    column_number,
                    data_list,
                    cell_format,
                )
                column_number += 1
                row_number = start_row_for_df

            self._workbook.close()

    def _fill_file_data(self):
        for parameter_type_instance in self._parameter_types_to_fill_data:
            tprm_group_name = (
                parameter_type_instance.group
                if parameter_type_instance.group
                else ""
            )

            self._count_tprm_by_groups(tprm_group_name=tprm_group_name)

            values_for_column = (
                self._get_parameters_by_parameter_type_for_column(
                    parameter_type_id=parameter_type_instance.id
                )
            )

            column_data = self._fill_column_data(
                param_type_instance=parameter_type_instance,
                prm_values=values_for_column,
            )

            column_data = self._convert_values_to_names(
                parameter_type_instance=parameter_type_instance,
                column_data=column_data,
            )

            self._main_df = self._main_df.assign(
                **{parameter_type_instance.name: column_data}
            )

            if not parameter_type_instance.multiple:
                self._cache_column_name_type[parameter_type_instance.name] = (
                    parameter_type_instance.val_type
                )

            self._groups_and_tprm_count[tprm_group_name] += 1

        self._fill_parent_name_column()
        self._fill_points_column()
        self._fill_all_object_attributes_column()

    def execute(self):
        self._get_query_params()
        self._get_parameter_type_to_filter()
        self._get_object_id_to_filter()
        self._fill_file_data()
        self._save_csv_file()
        self._save_xlsx_file()
        self._output.seek(0)

        return self._output
