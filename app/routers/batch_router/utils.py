import json
from collections import defaultdict
from typing import List

from fastapi import (
    Form,
)
from fastapi.exceptions import RequestValidationError
from pandas import DataFrame

from models import TPRM
from routers.batch_router.constants import (
    MO_ID_COLUMN,
    COLUMN_WITH_ORIGINAL_INDEXES,
)
from routers.batch_router.schemas import (
    BatchPreviewErrorInstance,
    BatchPreviewWarningInstance,
)


def parse_column_name_mapping(column_name_mapping: str = Form(default="{}")):
    if column_name_mapping is None:
        return {}
    try:
        return json.loads(column_name_mapping)
    except json.JSONDecodeError:
        raise RequestValidationError(
            [
                {
                    "loc": ("body", "column_name_mapping"),
                    "msg": "value is not a valid JSON",
                    "type": "type_error.json",
                }
            ]
        )


class BatchFileConstants:
    def __init__(self):
        self._created_mo_prms: DataFrame = DataFrame(
            columns=[
                MO_ID_COLUMN,
                COLUMN_WITH_ORIGINAL_INDEXES,
                "value",
                "tprm_id",
            ]
        )

        self._updated_parameters: DataFrame = DataFrame(
            columns=[
                MO_ID_COLUMN,
                COLUMN_WITH_ORIGINAL_INDEXES,
                "old_value",
                "new_value",
                "tprm_id",
                "prm_id",
            ]
        )

        self._created_attributes: DataFrame = DataFrame(
            columns=[
                MO_ID_COLUMN,
                COLUMN_WITH_ORIGINAL_INDEXES,
                "value",
                "attr_name",
            ]
        )

        self._updated_object_attributes: DataFrame = DataFrame(
            columns=[
                MO_ID_COLUMN,
                COLUMN_WITH_ORIGINAL_INDEXES,
                "old_value",
                "new_value",
                "attr_name",
            ]
        )

        self._deleted_object_values: DataFrame = DataFrame(
            columns=[
                MO_ID_COLUMN,
                COLUMN_WITH_ORIGINAL_INDEXES,
                "old_value",
                "attr_name",
            ]
        )


class BatchErrorAndWarningsCollector:
    def __init__(self):
        self._error_row_with_reasons: dict[
            str, list[BatchPreviewErrorInstance]
        ] = defaultdict(list)
        self._warning_indexes: dict[str, list[BatchPreviewWarningInstance]] = (
            dict()
        )


class BatchConstantVariables:
    def __init__(self, columns_to_drop: list[str] = None):
        self._dominant_types: List[str] = list()
        self._FILE_EMPTY_VALUES = ["", None]

        self._new_column_name_mapping: dict[str, str] = dict()
        self._columns_to_drop = [
            "label",
            "name",
            "id",
            "tmo_id",
            "creation_date",
            "modification_date",
            "version",
        ]
        self._mo_attributes_available_for_batch: set[str] = {
            "active",
            "pov",
            "geometry",
            "p_id",
            "parent_name",
            "point_a_name",
            "point_b_name",
            "description",
            "longitude",
            "latitude",
            "status",
            "document_count",
            "model",
            "point_a_id",
            "point_b_id",
        }
        self._formula_tprm_ids: list[str] = list()
        self._prm_link_tprms: dict[int, TPRM] = dict()
        self._mo_link_tprms: dict[int, TPRM] = dict()
        self._two_way_mo_link_tprms = list()
        self._prm_link_tprms_with_prm_link_filter = dict()
        self._primary_tprms: list[str] = list()
        self._mo_attrs_and_tprms: list[str] = list()
        self._tprm_instance_by_id: dict[int, TPRM] = dict()

        if columns_to_drop:
            self._columns_to_drop.extend(columns_to_drop)
