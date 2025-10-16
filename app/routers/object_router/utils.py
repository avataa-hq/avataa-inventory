import copy
import csv
import io
import json
import pickle
import re
from collections import namedtuple, defaultdict
from datetime import datetime
from typing import (
    List,
    Union,
    Optional,
    Literal,
    TypeAlias,
    Iterator,
    Any,
    Iterable,
)

from fastapi import HTTPException, Response
from geopy.distance import geodesic as GD
from sqlalchemy import (
    String,
    Integer,
    Date,
    DateTime,
    and_,
    or_,
    select,
    cast,
    Numeric,
    func,
    delete,
    false,
    text,
    bindparam,
)
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import (
    Session,
    select as select_sqlmodel,
)  # Import select from sqlmodel to return models instead of rows
from starlette.datastructures import QueryParams

from common.common_constant import NAME_DELIMITER
from common.common_schemas import OrderByRule
from common.common_utils import ValueTypeValidator
from database import SQLALCHEMY_LIMIT, get_chunked_values_by_sqlalchemy_limit
from functions import functions_dicts
from functions.db_functions import db_read
from functions.db_functions.db_delete import (
    delete_mo_links_by_mo_id,
    delete_prm_links_by_mo_id,
    delete_mo_links_by_mo_id_list,
    delete_prm_links_by_mo_id_list,
)
from functions.functions_utils import utils
from models import TPRM, PRM, MO, TMO
from routers.object_router.exceptions import DescendantsLimit, ObjectNotExists
from routers.object_type_router.utils import ObjectTypeDBGetter
from routers.parameter_router.schemas import GroupedParam, PRMReadMultiple

dict_convert_tmo_type_db_colum_type = {
    "str": String,
    "date": Date,
    "datetime": DateTime,
    "float": Numeric,
    "int": Numeric,
    "bool": String,
    "mo_link": Numeric,
    "prm_link": String,
    "user_link": String,
    "formula": String,
}

DATE_PATTERN = "%Y-%m-%d"
DATETIME_PATTERN = "%Y-%m-%dT%H:%M"

dict_operators_for_filter_condition = {"AND": and_, "OR": or_}


def str_contains_where_condition(db_column, value):
    """Returns 'where' condition where  values of 'db_column' contain 'value'."""
    value = f"%{value.lower()}%"
    return db_column.ilike(value)


def str_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""

    return db_column == value


def str_not_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""

    return db_column != value


def str_starts_with_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' start with 'value'."""

    return db_column.ilike(f"{value}%")


def str_ends_with_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' end with 'value'."""

    return db_column.ilike(f"%{value}")


def str_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def str_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


def str_is_any_of_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equals to any of values in 'value'."""
    values = [db_column.ilike(x.lower()) for x in value.split(";")]
    return or_(*values)


str_flags = {
    "contains": str_contains_where_condition,
    "equals": str_equals_where_condition,
    "not_equals": str_not_equals_where_condition,
    "starts_with": str_starts_with_where_condition,
    "ends_with": str_ends_with_where_condition,
    "is_empty": str_is_empty_where_condition,
    "is_not_empty": str_is_not_empty_where_condition,
    "is_any_of": str_is_any_of_where_condition,
}


def int_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    value = int(value)
    return db_column == value


def int_not_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    value = int(value)
    return db_column != value


def int_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def int_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


def int_is_any_of_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equals to any of the values in 'value'."""
    values = value.split(";")
    values = [int(x) for x in values]
    return db_column.in_(values)


def int_more_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value'."""
    value = int(value)
    return db_column > value


def int_more_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value' or equals 'value'."""
    value = int(value)
    return db_column >= value


def int_less_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value'."""
    value = int(value)
    return db_column < value


def int_less_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value' or equals 'value'."""
    value = int(value)
    return db_column <= value


int_flags = {
    "equals": int_equals_where_condition,
    "not_equals": int_not_equals_where_condition,
    "is_empty": int_is_empty_where_condition,
    "is_not_empty": int_is_not_empty_where_condition,
    "is_any_of": int_is_any_of_where_condition,
    "more": int_more_than_where_condition,
    "more_or_eq": int_more_than_or_equals_where_condition,
    "less": int_less_than_where_condition,
    "less_or_eq": int_less_than_or_equals_where_condition,
}


def float_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    value = float(value)
    return db_column == value


def float_not_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    value = float(value)
    return db_column != value


def float_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def float_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


def float_is_any_of_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equals to any of the values in 'value'."""
    values = value.split(";")
    values = [float(x) for x in values]
    return db_column.in_(values)


def float_more_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value'."""
    value = float(value)
    return db_column > value


def float_more_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value' or equals 'value'."""
    value = float(value)
    return db_column >= value


def float_less_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value'."""
    value = float(value)
    return db_column < value


def float_less_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value' or equals 'value'."""
    value = float(value)
    return db_column <= value


float_flags = {
    "equals": float_equals_where_condition,
    "not_equals": float_not_equals_where_condition,
    "is_empty": float_is_empty_where_condition,
    "is_not_empty": float_is_not_empty_where_condition,
    "is_any_of": float_is_any_of_where_condition,
    "more": float_more_than_where_condition,
    "more_or_eq": float_more_than_or_equals_where_condition,
    "less": float_less_than_where_condition,
    "less_or_eq": float_less_than_or_equals_where_condition,
}


def date_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    value = datetime.strptime(value, DATE_PATTERN).date()
    return db_column == value


def date_not_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    value = datetime.strptime(value, DATE_PATTERN).date()
    return db_column != value


def date_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def date_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


def date_is_any_of_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equals to any of the values in 'value'."""
    values = value.split(";")
    values = [datetime.strptime(x, DATE_PATTERN).date() for x in values]
    return db_column.in_(values)


def date_more_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value'."""
    value = datetime.strptime(value, DATE_PATTERN).date()
    return db_column > value


def date_more_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value' or equals 'value'."""
    value = datetime.strptime(value, DATE_PATTERN).date()
    return db_column >= value


def date_less_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value'."""
    value = datetime.strptime(value, DATE_PATTERN).date()
    return db_column < value


def date_less_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value' or equals 'value'."""
    value = datetime.strptime(value, DATE_PATTERN).date()
    return db_column <= value


date_flags = {
    "equals": date_equals_where_condition,
    "not_equals": date_not_equals_where_condition,
    "is_empty": date_is_empty_where_condition,
    "is_not_empty": date_is_not_empty_where_condition,
    "is_any_of": date_is_any_of_where_condition,
    "more": date_more_than_where_condition,
    "more_or_eq": date_more_than_or_equals_where_condition,
    "less": date_less_than_where_condition,
    "less_or_eq": date_less_than_or_equals_where_condition,
}


def datetime_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    value = datetime.strptime(value, DATETIME_PATTERN)
    return db_column == value


def datetime_not_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    value = datetime.strptime(value, DATETIME_PATTERN)
    return db_column != value


def datetime_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def datetime_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


def datetime_is_any_of_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equals to any of the values in 'value'."""
    values = value.split(";")
    values = [datetime.strptime(x, DATETIME_PATTERN) for x in values]
    return db_column.in_(values)


def datetime_more_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value'."""
    value = datetime.strptime(value, DATETIME_PATTERN)
    return db_column > value


def datetime_more_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' more than 'value' or equals 'value'."""
    value = datetime.strptime(value, DATETIME_PATTERN)
    return db_column >= value


def datetime_less_than_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value'."""
    value = datetime.strptime(value, DATETIME_PATTERN)
    return db_column < value


def datetime_less_than_or_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' less than 'value' or equals 'value'."""
    value = datetime.strptime(value, DATETIME_PATTERN)
    return db_column <= value


datetime_flags = {
    "equals": datetime_equals_where_condition,
    "not_equals": datetime_not_equals_where_condition,
    "is_empty": datetime_is_empty_where_condition,
    "is_not_empty": datetime_is_not_empty_where_condition,
    "is_any_of": datetime_is_any_of_where_condition,
    "more": datetime_more_than_where_condition,
    "more_or_eq": datetime_more_than_or_equals_where_condition,
    "less": datetime_less_than_where_condition,
    "less_or_eq": datetime_less_than_or_equals_where_condition,
}


def bool_equals_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    match value:
        case "true":
            return db_column == "True"
        case "false":
            return db_column == "False"
        case _:
            return or_(db_column == "False", db_column == "True")


def bool_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def bool_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


def bool_is_true_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal to true."""

    return db_column == "True"


def bool_is_false_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' equal to false."""

    return db_column == "False"


bool_flags = {
    "is_empty": bool_is_empty_where_condition,
    "is_not_empty": bool_is_not_empty_where_condition,
    "equals": bool_equals_where_condition,
    "is_true": bool_is_true_where_condition,
    "is_false": bool_is_false_where_condition,
}


def prm_is_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    return db_column == None  # noqa


def prm_is_not_empty_where_condition(db_column, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    return db_column != None  # noqa


prm_flag = {
    "is_empty": prm_is_empty_where_condition,
    "is_not_empty": prm_is_not_empty_where_condition,
}

dict_of_filter_flags = {
    "str": str_flags,
    "date": date_flags,
    "datetime": datetime_flags,
    "float": float_flags,
    "int": int_flags,
    "bool": bool_flags,
    "mo_link": int_flags,
    "prm_link": prm_flag,
    "user_link": str_flags,
    "formula": str_flags,
}


class TPRMFilterCleaner:
    def __init__(
        self,
        session: Session,
        query_params=QueryParams(),
        regex_pattern: str = r"tprm_id(\d+)\|([^=]+)",
        object_type_id: int = None,
    ):
        self.parse_pattern = re.compile(regex_pattern)
        self.session = session
        self.query_params = query_params
        self._filter_dict = None
        self.object_type_id = object_type_id
        self._clean_filter_dict = None
        self._filter_logical_operator = None

    @property
    def filter_logical_operator(self):
        if self._filter_logical_operator is not None:
            return self._filter_logical_operator

        filter_logical_operator = self.query_params.get(
            "filter_logical_operator", "and"
        )
        allowed_operators = {"and": and_, "or": or_}

        if filter_logical_operator in allowed_operators:
            self._filter_logical_operator = allowed_operators[
                filter_logical_operator
            ]
        else:
            self._filter_logical_operator = and_

        return self._filter_logical_operator

    @staticmethod
    def __field_value_validation_by_val_type(val_type, filter_flag, value):
        def any_value(data, flag):
            return True

        def date_validator(data, flag):
            if flag == "is_any_of":
                try:
                    [
                        datetime.strptime(v, DATE_PATTERN).date()
                        for v in data.split(";")
                    ]
                except ValueError:
                    return False
                else:
                    return True
            else:
                try:
                    datetime.strptime(data, DATE_PATTERN).date()
                except ValueError:
                    return False
                else:
                    return True

        def datetime_validator(data, flag):
            if flag == "is_any_of":
                try:
                    [
                        datetime.strptime(v, DATETIME_PATTERN)
                        for v in data.split(";")
                    ]
                except ValueError:
                    return False
                else:
                    return True
            else:
                try:
                    datetime.strptime(data, DATETIME_PATTERN)
                except ValueError:
                    return False
                else:
                    return True

        def int_validator(data, flag):
            if flag == "is_any_of":
                try:
                    [int(v) for v in data.split(";")]
                except ValueError:
                    return False
                else:
                    return True
            else:
                try:
                    int(data)
                except ValueError:
                    return False
                else:
                    return True

        def float_validator(data, flag):
            if flag == "is_any_of":
                try:
                    [float(v) for v in data.split(";")]
                except ValueError:
                    return False
                else:
                    return True
            else:
                try:
                    float(data)
                except ValueError:
                    return False
                else:
                    return True

        validation_by_val_type = {
            "str": any_value,
            "date": date_validator,
            "datetime": datetime_validator,
            "float": float_validator,
            "int": int_validator,
            "bool": any_value,
            "mo_link": int_validator,
            "prm_link": any_value,
            "user_link": any_value,
            "formula": any_value,
        }

        if filter_flag in ["is_empty", "is_not_empty"]:
            return True

        validator = validation_by_val_type.get(val_type, False)
        if validator is False:
            return False

        return validator(data=value, flag=filter_flag)

    def check_filter_data_in_query_params(self):
        """Returns True if there are filter data in query_params otherwise False"""
        res = self.parse_pattern.findall("=".join(self.query_params.keys()))
        return len(res) > 0

    @property
    def filter_dict(self):
        """Returns dict of tprm_id, filter flag and filter value if there are particular data in query_params.
        result = {tprm_id: {(tprm_id, filter_flag) : filter_value}}
        """
        if self._filter_dict is not None:
            return self._filter_dict

        regex_equal = re.compile(r"tprm_id(\d+)\|([^=]+)")
        filters = dict()

        for key, value in self.query_params.multi_items():
            match = regex_equal.fullmatch(key)
            if match:
                match_values = regex_equal.findall(key)[0]
                tprm_id = int(match_values[0])

                if filters.get(tprm_id, None) is None:
                    filters[tprm_id] = set()

                unique_combination = namedtuple("Key", "Id Flag Value")
                unique_combination = unique_combination(
                    tprm_id, match_values[1], value
                )
                filters[tprm_id].add(unique_combination)
                # filters[tprm_id][match_values] = value
        self._filter_dict = filters
        return filters

    @property
    def clean_filter_dict(self):
        """Returns cleaned dict of tprm_id, filter flag and filter value if there are particular data in query_params
        that can be used in a filter process.
        result = {tprm_id: {(tprm_id, filter_flag) : filter_value}}
        """
        if self._clean_filter_dict is not None:
            return self._clean_filter_dict

        object_type_where_condition = []
        if self.object_type_id is not None:
            object_type_where_condition.append(
                TPRM.tmo_id == self.object_type_id
            )

        tprm_ids = list(self.filter_dict.keys())

        stmt = select(TPRM).where(
            TPRM.id.in_(tprm_ids),
            TPRM.multiple == False,  # noqa
            *object_type_where_condition,
        )
        tprms = self.session.execute(stmt).all()

        cleaned_filter_dict = {}

        for tprm in tprms:
            tprm = tprm[0]
            unique_key = namedtuple("UniqueKey", "Id VatType")
            unique_key = unique_key(tprm.id, tprm.val_type)

            for combination in self.filter_dict[tprm.id]:
                is_valid = self.__field_value_validation_by_val_type(
                    val_type=tprm.val_type,
                    filter_flag=combination.Flag,
                    value=combination.Value,
                )
                if is_valid:
                    if cleaned_filter_dict.get(unique_key, False) is False:
                        cleaned_filter_dict[unique_key] = set()

                    cleaned_filter_dict[unique_key].add(combination)

        self._clean_filter_dict = cleaned_filter_dict

        return cleaned_filter_dict

    def get_mo_ids_which_match_clean_filter_conditions(
        self,
        obj_ids=None,
        p_id=None,
        order_by: dict | None = None,
        active: bool | None = None,
    ):
        stm = select(MO.id, MO.tmo_id, MO.p_id)

        mo_where_condition = []
        # add start where condition - start
        if self.object_type_id is not None:
            mo_where_condition.append(MO.tmo_id == self.object_type_id)
        if obj_ids is not None:
            mo_where_condition.append(MO.id.in_(obj_ids))
        if p_id is not None:
            if isinstance(p_id, list):
                mo_where_condition.append(MO.p_id.in_(p_id))
            else:
                mo_where_condition.append(MO.p_id == p_id)
        # add start where condition - end
        stm = stm.where(*mo_where_condition)

        if self.clean_filter_dict:
            where_condition = []
            joins_tables = []

            iter_for_label_name = 1
            for (
                combined_key,
                set_of_combined_data,
            ) in self.clean_filter_dict.items():
                param_label = f"param_{iter_for_label_name}"
                cast_type = dict_convert_tmo_type_db_colum_type.get(
                    combined_key.VatType, False
                )
                cache_to_check_param_exist = {}

                flags_for_current_val_type = dict_of_filter_flags.get(
                    combined_key.VatType, False
                )

                for combined_data in set_of_combined_data:
                    where_condition_for_flag = False
                    if flags_for_current_val_type:
                        where_condition_for_flag = (
                            flags_for_current_val_type.get(
                                combined_data.Flag, False
                            )
                        )

                    if all([cast_type, where_condition_for_flag]):
                        if param_label not in cache_to_check_param_exist:
                            column_name = f"filter_{combined_data.Id}"

                            select_statement = select(
                                PRM.mo_id,
                                cast(PRM.value, cast_type()).label(column_name),
                            ).where(PRM.tprm_id == combined_data.Id)
                            aliased_table = aliased(select_statement.subquery())

                            joins_tables.append(aliased_table)

                            cache_to_check_param_exist[param_label] = (
                                aliased_table
                            )

                        else:
                            aliased_table = cache_to_check_param_exist[
                                param_label
                            ]

                        where_condition.append(
                            where_condition_for_flag(
                                getattr(aliased_table.c, column_name),
                                combined_data.Value,
                            )
                        )
            for table in joins_tables:
                stm = stm.outerjoin(table, MO.id == table.c.mo_id)

            stm = stm.where(
                self.filter_logical_operator(*where_condition)
            ).distinct()

        if order_by:
            order_value = namedtuple("OrderValue", "type ascending")

            for order_tprm_id, order_tprm_value in order_by.items():
                column_name = f"order_{order_tprm_id}"
                order_tprm_value: order_value = order_tprm_value
                cast_type = dict_convert_tmo_type_db_colum_type.get(
                    order_tprm_value.type, False
                )
                if not cast_type:
                    continue

                select_statement = select(
                    PRM.mo_id, cast(PRM.value, cast_type()).label(column_name)
                ).where(PRM.tprm_id == order_tprm_id)
                aliased_table = aliased(select_statement.subquery())

                order_by_condition = getattr(aliased_table.c, column_name)
                if not order_tprm_value.ascending:
                    order_by_condition = order_by_condition.desc()

                stm = (
                    stm.outerjoin(aliased_table, MO.id == aliased_table.c.mo_id)
                    .order_by(order_by_condition)
                    .add_columns(order_by_condition)
                )
        else:
            stm = stm.order_by(MO.id)

        if active is not None:
            stm = stm.where(MO.active == active)

        res = self.session.execute(stm).scalars().all()
        return res


def filter_flags_by_tprm(tprm: TPRM):
    """Returns dict with filter flags particular tprm_id"""
    flags = dict_of_filter_flags.get(tprm.val_type, None)
    if flags is not None:
        return list(flags.keys())
    else:
        return []


def recursive_find_children_all_children_tmo(
    tmo_ids: List[int], session: Session
) -> list[int]:
    """Returns ordered list of all children object types ids for tmo_ids."""
    stmt = select(TMO.id).where(TMO.p_id.in_(tmo_ids))
    children_tmo_ids = session.execute(stmt).scalars().all()
    if children_tmo_ids:
        return children_tmo_ids + recursive_find_children_all_children_tmo(
            children_tmo_ids, session
        )
    else:
        return children_tmo_ids


def validate_sequence_value_if_constraint(
    session, param_type_info, sequence_type, user_value
):
    if not sequence_type:
        return

    query = select(func.count(PRM.id)).where(
        PRM.tprm_id == param_type_info.constraint,
        PRM.value == str(sequence_type),
    )
    sequence_value = session.execute(query)
    sequence_value = sequence_value.scalar()

    if int(user_value) > sequence_value + 1:
        raise HTTPException(
            status_code=422,
            detail=f"Incorrect value for sequence parameter. "
            f"Value exceeds sequence length for sequence type: "
            f"{sequence_type}!",
        )


def get_value_for_sequence(session, param_type_info, sequence_type=None):
    query = select(func.count(PRM.id)).where(PRM.tprm_id == param_type_info.id)
    if sequence_type:
        type_subquery = (
            select(PRM.mo_id)
            .where(
                PRM.tprm_id == param_type_info.constraint,
                PRM.value == str(sequence_type),
            )
            .scalar_subquery()
        )
        query = query.where(PRM.mo_id.in_(type_subquery))

    sequence_value = session.execute(query)
    sequence_value = sequence_value.scalar()
    if not sequence_value:
        return 1
    else:
        return sequence_value + 1


def validate_object_parameters(
    session, parameter, object_instance, other_params
):
    parameter_type_instance = db_read.get_db_param_type_or_exception_422(
        session=session, tprm_id=parameter.tprm_id
    )

    if parameter_type_instance.tmo_id != object_instance.tmo_id:
        raise HTTPException(
            status_code=422,
            detail=f"This object has no parameter type with such id ({parameter_type_instance.id}).",
        )

    if (
        parameter.value is None
        and parameter_type_instance.val_type != "sequence"
    ):
        raise HTTPException(
            status_code=422, detail="Please, pass the param value."
        )

    validation_task = ValueTypeValidator(
        session=session,
        parameter_type_instance=parameter_type_instance,
        value_to_validate=parameter.value,
    )
    validation_task.validate()


def proceed_parameter_attributes(parameter_instance: PRM):
    db_param_dict = parameter_instance.dict()
    if parameter_instance.tprm.multiple:
        multiple_value = utils.decode_multiple_value(
            value=parameter_instance.value
        )
        db_param_dict["value"] = multiple_value
        param_to_read = PRMReadMultiple(**db_param_dict)
    else:
        param_to_read = functions_dicts.db_param_convert_by_val_type[
            parameter_instance.tprm.val_type
        ](**db_param_dict)
    return param_to_read


def get_grouped_params(
    param_types: List[TPRM],
    grouped_filled_params: dict,
    only_filled: bool,
    grouped_result: dict,
    db_object: MO,
) -> dict[str, List[GroupedParam]]:
    for param_type in param_types:
        param_type_result = param_type.dict()
        param_type_result.pop("version")
        if param_type.id in grouped_filled_params:
            param_type_result.pop("id")  # Drop
            filled_params = grouped_filled_params[param_type.id]
            filled_params_result = []
            for filled_param in filled_params:
                param = filled_param.dict()
                param["prm_id"] = param.pop("id")  # Rename
                filled_params_result.append(
                    GroupedParam(**param_type_result, **param)
                )
            grouped_result[param_type.group].extend(filled_params_result)
        elif not only_filled:
            empty_param = {
                "prm_id": None,
                "version": 0,
                "value": None,
                "mo_id": db_object.id,
                "tprm_id": param_type_result.pop("id"),
            }
            grouped_result[param_type.group].append(
                GroupedParam(**param_type_result, **empty_param)
            )
    return grouped_result


def proceed_object_delete(session: Session, object_instance_to_delete: MO):
    delete_mo_links_by_mo_id(session=session, mo=object_instance_to_delete)
    delete_prm_links_by_mo_id(
        session=session, mo_id=object_instance_to_delete.id
    )
    collapse_sequences_if_exist(session, object_instance_to_delete)
    session.delete(object_instance_to_delete)


def proceed_object_list_delete(
    session: Session,
    object_instances: List[MO] | set[MO],
    object_type_ids: List[str] | set[str],
):
    mo_ids = {mo.id for mo in object_instances}

    delete_mo_links_by_mo_id_list(
        session=session,
        mo_list=object_instances,
        object_type_ids=object_type_ids,
    )

    delete_prm_links_by_mo_id_list(session=session, mo_ids=mo_ids)

    for mo in object_instances:
        collapse_sequences_if_exist(session, mo)
        session.delete(mo)


def concat_order_by(
    session: Session,
    order_by_tprms_id: list[int] | int | None,
    order_by_asc: list[bool] | bool | None,
):
    default_asc = True

    if order_by_tprms_id is None or len(order_by_tprms_id) == 0:
        return

    is_empty_list = (
        len(order_by_tprms_id) == 0
        if isinstance(order_by_tprms_id, list)
        else False
    )
    if is_empty_list:
        return

    if not isinstance(order_by_tprms_id, list):
        order_by_tprms_id = [order_by_tprms_id]

    if not isinstance(order_by_asc, list):
        order_by_asc = [order_by_asc]
    order_by_asc = list(
        map(lambda x: default_asc if x is None else x, order_by_asc)
    )

    if len(order_by_asc) > 1 and len(order_by_asc) != len(order_by_tprms_id):
        return

    if len(order_by_asc) == 1 and len(order_by_tprms_id) > 1:
        order_by_asc = [order_by_asc[0] for _ in order_by_tprms_id]

    order_by = dict(zip(order_by_tprms_id, order_by_asc))

    query = select(TPRM).where(
        TPRM.id.in_(order_by.keys()), TPRM.multiple == false()
    )
    exists_tprms = session.execute(query).scalars().all()

    order_by_res = {}
    order_value = namedtuple("OrderValue", "type ascending")
    for exists_tprm in exists_tprms:
        ascending = order_by[exists_tprm.id]
        val_type = exists_tprm.val_type
        order_item = order_value(type=val_type, ascending=ascending)
        order_by_res[exists_tprm.id] = order_item
    return order_by_res


def ilike(pattern: str, string_list: list) -> list:
    pattern_lower = pattern.lower()

    return any(pattern_lower in string.lower() for string in string_list)


def decode_pickle_data(data: str):
    return pickle.loads(bytes.fromhex(data))


def get_conditions_for_coords(
    outer_box_longitude_min: Union[float, None],
    outer_box_longitude_max: Union[float, None],
    outer_box_latitude_min: Union[float, None],
    outer_box_latitude_max: Union[float, None],
    inner_box_longitude_min: Union[float, None],
    inner_box_longitude_max: Union[float, None],
    inner_box_latitude_max: Union[float, None],
    inner_box_latitude_min: Union[float, None],
) -> List:
    """Returns select condition with limits by coordinates"""
    outer_box_coords = [
        outer_box_longitude_min,
        outer_box_longitude_max,
        outer_box_latitude_min,
        outer_box_latitude_max,
    ]
    if any(outer_box_coords):
        if not all(outer_box_coords):
            raise HTTPException(
                status_code=422,
                detail="No data is required for outer_box. "
                "But if at least one data was entered for the outer box, "
                "all other data must be specified too.",
            )

    inner_box_coords = [
        inner_box_longitude_min,
        inner_box_longitude_max,
        inner_box_latitude_min,
        inner_box_latitude_max,
    ]
    if any(inner_box_coords):
        if not all(inner_box_coords):
            raise HTTPException(
                status_code=422,
                detail="No data is required for inner_box. "
                "But if at least one data was entered for the inner box, "
                "all other data must be specified too.",
            )
    conditions = []
    if all(outer_box_coords):
        if not all(inner_box_coords):
            conditions = [
                MO.longitude >= outer_box_longitude_min,
                MO.longitude <= outer_box_longitude_max,
                MO.latitude >= outer_box_latitude_min,
                MO.latitude <= outer_box_latitude_max,
            ]
            return conditions
        else:
            # check if inner box in outer box
            inner_box_in_outer = True

            if (
                outer_box_longitude_min > inner_box_longitude_max
                or outer_box_latitude_min > inner_box_latitude_max
            ):
                inner_box_in_outer = False

            if inner_box_in_outer is False:
                conditions = [
                    MO.longitude >= outer_box_longitude_min,
                    MO.longitude <= outer_box_longitude_max,
                    MO.latitude >= outer_box_latitude_min,
                    MO.latitude <= outer_box_latitude_max,
                ]
                return conditions

            longitude_or_conditions = []
            if outer_box_longitude_max > inner_box_longitude_max:
                longitude_or_conditions.append(
                    and_(
                        MO.longitude <= outer_box_longitude_max,
                        MO.longitude > inner_box_longitude_max,
                        MO.latitude >= outer_box_latitude_min,
                        MO.latitude <= outer_box_latitude_max,
                    )
                )
            if outer_box_longitude_min < inner_box_longitude_min:
                longitude_or_conditions.append(
                    and_(
                        MO.longitude >= outer_box_longitude_min,
                        MO.longitude < inner_box_longitude_min,
                        MO.latitude >= outer_box_latitude_min,
                        MO.latitude <= outer_box_latitude_max,
                    )
                )

            latitudes_or_conditions = []
            if outer_box_latitude_max > inner_box_latitude_max:
                latitudes_or_conditions.append(
                    and_(
                        MO.latitude <= outer_box_latitude_max,
                        MO.latitude > inner_box_latitude_max,
                        MO.longitude >= outer_box_longitude_min,
                        MO.longitude <= outer_box_longitude_max,
                    )
                )
            if outer_box_latitude_min < inner_box_latitude_min:
                latitudes_or_conditions.append(
                    and_(
                        MO.latitude >= outer_box_latitude_min,
                        MO.latitude < inner_box_latitude_min,
                        MO.longitude >= outer_box_longitude_min,
                        MO.longitude <= outer_box_longitude_max,
                    )
                )
            lat_lon_cond = latitudes_or_conditions + longitude_or_conditions

            conditions = [or_(*lat_lon_cond)]
    return conditions


def read_objects_with_params(
    session: Session,
    query_params: QueryParams,
    response: Response,
    object_type_id: int = None,
    p_id: int = None,
    name: str = None,
    obj_id: Union[List[int], None] = None,
    with_parameters: bool = False,
    active: bool = True,
    limit: Optional[int] = 50,
    offset: Optional[int] = 0,
    order_by_tprms_id: list[int] | None = None,
    order_by_asc: list[bool] | None = None,
    identifiers_instead_of_values: bool = False,
    search_rule: Literal["start_with", "end_with", "contains"] = "contains",
):
    if name:
        filters = [MO.latitude.isnot(None), MO.longitude.isnot(None)]

        if name != " ":
            ilike_rule = "%" + name + "%"

            if search_rule == "start_with":
                ilike_rule = name + "%"
            if search_rule == "end_with":
                ilike_rule = "%" + name

            filters = [
                MO.latitude.isnot(None),
                MO.longitude.isnot(None),
                MO.name.ilike(ilike_rule),
            ]

        if obj_id:
            filters.append(MO.id.in_(obj_id))

        query = select(MO.id).where(and_(*filters)).order_by(MO.name)
        obj_id = session.execute(query).scalars().all()

    if object_type_id is not None:
        db_read.get_db_object_type_or_exception(session, object_type_id)

    tprm_cleaner = TPRMFilterCleaner(
        session=session,
        query_params=query_params,
        object_type_id=object_type_id,
    )

    order_by = concat_order_by(
        session=session,
        order_by_tprms_id=order_by_tprms_id,
        order_by_asc=order_by_asc,
    )
    start = offset
    end = start + limit

    if tprm_cleaner.check_filter_data_in_query_params() or order_by:
        mos_ids = tprm_cleaner.get_mo_ids_which_match_clean_filter_conditions(
            obj_ids=obj_id, p_id=p_id, order_by=order_by, active=active
        )
        obj_ids = mos_ids[start:end]
        objects_to_read = db_read.get_objects_with_parameters_by_mo_ids(
            session=session,
            limit=None,
            offset=None,
            mos_ids=obj_ids,
            returnable=not with_parameters,
            active=active,
            identifiers_instead_of_values=identifiers_instead_of_values,
        )
        results_length = utils.count_objects(
            session=session,
            object_type_id=object_type_id,
            mos_ids=mos_ids,
            p_id=p_id,
            active=active,
        )
    else:
        obj_ids = obj_id
        if isinstance(obj_id, list):
            obj_ids = obj_id[start:end]
            offset = None
        else:
            offset = start
        objects_to_read = db_read.get_objects_with_parameters_by_mo_ids(
            session=session,
            limit=limit,
            offset=offset,
            object_type_id=object_type_id,
            mos_ids=obj_ids,
            p_id=p_id,
            returnable=not with_parameters,
            active=active,
            order_by_rule=OrderByRule(rule="asc"),
            identifiers_instead_of_values=identifiers_instead_of_values,
            with_parent_name=True,
        )
        results_length = utils.count_objects(
            session=session,
            object_type_id=object_type_id,
            mos_ids=obj_id,
            p_id=p_id,
            active=active,
        )
    response.headers["Result-Length"] = str(results_length)
    return objects_to_read


def update_child_location_mo(
    session: Session,
    object_instance: MO,
    object_type_instance: TMO,
    new_data: dict,
):
    stmt = select_sqlmodel(TMO).where(
        TMO.p_id == object_type_instance.id,
        TMO.geometry_type == object_type_instance.geometry_type,
    )
    list_child_tmo: list[TMO] = session.exec(stmt).all()
    for child_tmo in list_child_tmo:  # type: TMO
        if child_tmo.inherit_location:
            child_mo: list[MO] = session.exec(
                select_sqlmodel(MO).where(
                    MO.tmo_id == child_tmo.id, MO.p_id == object_instance.id
                )
            ).all()
            utils.set_location_attrs(
                session=session,
                db_param=child_tmo.geometry_type,
                child_mos=child_mo,
                set_value=True,
                location_data=new_data,
            )
    session.flush()


def update_geometry(
    object_instance: MO,
    point_a: MO | None = None,
    point_b: MO | None = None,
    repair_inner_coord: bool = False,
) -> dict:
    """Create/update correct geometry based on current values point_a and point_b
    GeoJSON compel format longitude-latitude for point
    https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.1"""
    temp_geometry = {
        "path": {
            "type": "LineString",
            "coordinates": [],
        },
        "path_length": 0,
    }
    if object_instance.geometry:
        # If path as string
        if isinstance(object_instance.geometry.get("path"), dict):
            temp_geometry: dict = object_instance.geometry
            coord = temp_geometry["path"].get("coordinates")
        elif isinstance(object_instance.geometry.get("path"), str):
            coord = json.loads(object_instance.geometry["path"])
        elif isinstance(object_instance.geometry.get("path"), list):
            coord = object_instance.geometry.get("path")
        else:
            raise ValueError("Incorrect data in geometry.")

        if point_a and point_a.latitude:
            start_latitude = point_a.latitude
        elif coord:
            start_latitude = coord[0][-1]
        else:
            start_latitude = None
        if point_a and point_a.longitude:
            start_longitude = point_a.longitude
        elif coord:
            start_longitude = coord[0][0]
        else:
            start_longitude = None
        if point_b and point_b.latitude:
            end_latitude = point_b.latitude
        elif coord:
            end_latitude = coord[-1][-1]
        else:
            end_latitude = None
        if point_b and point_b.longitude:
            end_longitude = point_b.longitude
        elif coord:
            end_longitude = coord[-1][0]
        else:
            end_longitude = None

        # Check if complex path for update (more than point_a, point_b)
        if coord[1:-1]:
            coord = [
                [start_longitude, start_latitude],
                *[[float(el[0]), float(el[1])] for el in coord[1:-1]],
                [end_longitude, end_latitude],
            ]
            if repair_inner_coord:
                coord = [
                    [start_longitude, start_latitude],
                    *[[float(el[1]), float(el[0])] for el in coord[1:-1]],
                    [point_b.longitude, point_b.latitude],
                ]
            path_len = GD(*[tuple([point[1], point[0]]) for point in coord]).km
        elif (
            start_latitude
            and start_longitude
            and end_latitude
            and end_longitude
        ):
            coord = [
                [start_longitude, start_latitude],
                [end_longitude, end_latitude],
            ]
            path_len = GD(*[tuple([point[1], point[0]]) for point in coord]).km
        else:
            coord = []
            path_len = 0
    else:
        cur_point_a_lon = _get_existed_coord(
            point_a, object_instance.point_a, "longitude"
        )
        cur_point_a_lat = _get_existed_coord(
            point_a, object_instance.point_a, "latitude"
        )
        cur_point_b_lon = _get_existed_coord(
            point_b, object_instance.point_b, "longitude"
        )
        cur_point_b_lat = _get_existed_coord(
            point_b, object_instance.point_b, "latitude"
        )
        if (
            cur_point_a_lat
            and cur_point_a_lon
            and cur_point_b_lat
            and cur_point_b_lon
        ):
            coord = [
                [cur_point_a_lon, cur_point_a_lat],
                [cur_point_b_lon, cur_point_b_lat],
            ]
            path_len = GD(*[tuple([point[1], point[0]]) for point in coord]).km
        else:
            coord = []
            path_len = 0
    # path_length compel lat/long
    # https://geopy.readthedocs.io/en/stable/#module-geopy.distance
    temp_geometry["path"]["coordinates"] = coord
    temp_geometry["path_length"] = path_len
    if temp_geometry["path"].get("path_length", None) is not None:
        del temp_geometry["path"]["path_length"]
    return temp_geometry


def _get_existed_coord(primary, fallback, attribute: str):
    if primary:
        result = getattr(primary, attribute)
    elif fallback:
        result = getattr(fallback, attribute)
    else:
        result = None
    return result


def reconstruct_geometry(
    session: Session, tmo_id: int, correct: bool
) -> io.StringIO:
    stmt = (
        select(MO)
        .where(
            and_(
                MO.tmo_id == tmo_id,
                MO.point_a_id.isnot(None),
                MO.point_b_id.isnot(None),
            )
        )
        .execution_options(yield_per=1000)
    )
    field_names = [
        "id",
        "geometry_before",
        "geometry_after",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(
        output, quoting=csv.QUOTE_NONNUMERIC, fieldnames=field_names
    )
    writer.writeheader()
    for partition in session.scalars(stmt).partitions():
        for mo in partition:  # type: MO
            old_geometry = str(mo.geometry)
            if (
                mo.point_a.latitude
                and mo.point_a.longitude
                and mo.point_b.latitude
                and mo.point_b.longitude
            ):
                mo.geometry = update_geometry(
                    object_instance=mo, point_a=mo.point_a, point_b=mo.point_b
                )
                mo.version += 1
                flag_modified(mo, "geometry")

                session.add(mo)
                writer.writerow(
                    {
                        "id": mo.id,
                        "geometry_before": old_geometry,
                        "geometry_after": str(mo.geometry),
                    }
                )
            else:
                writer.writerow(
                    {
                        "id": mo.id,
                        "geometry_before": old_geometry,
                        "geometry_after": old_geometry,
                    }
                )
    if correct:
        session.commit()
    return output


def collapse_sequence_for_tprm(session: Session, param_type: TPRM, mo_id: int):
    query = select(PRM).where(PRM.tprm_id == param_type.id, PRM.mo_id == mo_id)

    sequence_prm = session.execute(query)
    sequence_prm = sequence_prm.scalar()
    if not sequence_prm:
        return

    query = select(PRM).where(
        PRM.tprm_id == sequence_prm.tprm_id,
        cast(PRM.value, Integer) >= int(sequence_prm.value),
    )
    if param_type.constraint:
        sequence_type_value_subquery = (
            select(PRM.value)
            .where(PRM.tprm_id == param_type.constraint, PRM.mo_id == mo_id)
            .scalar_subquery()
        )
        sequence_type_tprm_subquery = (
            select(PRM.tprm_id)
            .where(PRM.tprm_id == param_type.constraint, PRM.mo_id == mo_id)
            .scalar_subquery()
        )

        subquery = (
            select(PRM.mo_id)
            .where(
                PRM.value == sequence_type_value_subquery,
                PRM.tprm_id == sequence_type_tprm_subquery,
            )
            .scalar_subquery()
        )
        query = query.where(PRM.mo_id.in_(subquery))

    query = query.execution_options(yield_per=100)
    params_to_update = session.execute(query).scalars().partitions(100)
    for chunk in params_to_update:
        for param_to_update in chunk:
            param_to_update.value = str(int(param_to_update.value) - 1)
            session.add(param_to_update)

    query = delete(PRM).where(PRM.tprm_id == param_type.id, PRM.mo_id == mo_id)
    session.execute(query)


def collapse_sequences_if_exist(session: Session, object_instance: MO):
    query = select(TPRM).where(
        TPRM.tmo_id == object_instance.tmo_id, TPRM.val_type == "sequence"
    )
    sequence_tprms = session.exec(query)
    sequence_tprms = sequence_tprms.scalars().all()
    if not sequence_tprms:
        return

    for seq_tprm in sequence_tprms:
        collapse_sequence_for_tprm(session, seq_tprm, object_instance.id)


def collapse_sequences_if_exist_massive(session: Session, mo: List[MO]):
    query = select(TPRM).where(
        TPRM.tmo_id == mo.tmo_id, TPRM.val_type == "sequence"
    )
    sequence_tprms = session.exec(query)
    sequence_tprms = sequence_tprms.scalars().all()
    if not sequence_tprms:
        return

    for seq_tprm in sequence_tprms:
        collapse_sequence_for_tprm(session, seq_tprm, mo.id)


def get_updated_object_names(
    session: Session,
    global_new_object_names: dict[int, str],
    mo_link_tprm_ids: list[int],
    object_type_data: TMO,
    objects_for_update: list[MO],
) -> dict[int, str]:
    """
    This method will update names of objects, which will get in "object_type_data".
    It will be done by primary of his TMO and parent name

    In result this method will return dict, where:
        keys: are ids of object, we need updated
                (there are can be objects not only by list of objects, which were in request,
                but object, which can be child for them)
        values: new names for objects

    """
    new_object_names_in_scope = {}
    mo_and_his_parent_name = {}
    already_updated_objects = set(global_new_object_names.keys())
    obj_name_by_primary = {obj.id: [] for obj in objects_for_update}
    object_ids_need_to_be_updated = set(obj_name_by_primary.keys())

    # to escapee double changing name for object -
    # we need remove from list of object, we want to update - already updated
    object_ids_need_to_be_updated = object_ids_need_to_be_updated.difference(
        already_updated_objects
    )

    for tprm_id in object_type_data.primary:
        for obj_ids_for_updated_chunk in get_chunked_values_by_sqlalchemy_limit(
            object_ids_need_to_be_updated
        ):
            # GET PARAMS OF CURRENT PRIMARY TPRM
            conditions = [
                PRM.mo_id.in_(obj_ids_for_updated_chunk),
                PRM.tprm_id == tprm_id,
            ]
            mo_and_primary_parameter = session.execute(
                select(PRM.mo_id, PRM.value).where(*conditions)
            ).all()

            # FORMAT MO_LINK IDS TO NAMES
            if tprm_id in mo_link_tprm_ids:
                mo_and_linked_id = {
                    mo_id: int(linked_mo_id)
                    for mo_id, linked_mo_id in mo_and_primary_parameter
                }

                for mo_id, linked_id in mo_and_linked_id.items():
                    if linked_id not in already_updated_objects:
                        linked_mo_data = session.execute(
                            select(MO).where(MO.id == linked_id)
                        ).scalar()
                        linked_tmo_data = session.execute(
                            select(TMO).where(TMO.id == linked_mo_data.tmo_id)
                        ).scalar()

                        tprms_with_val_type_mo_link = (
                            session.execute(
                                select(TPRM.id).where(
                                    TPRM.id.in_(linked_tmo_data.primary),
                                    TPRM.val_type == "mo_link",
                                )
                            )
                            .scalars()
                            .all()
                        )

                        updated_objects = get_updated_object_names(
                            session=session,
                            global_new_object_names=global_new_object_names,
                            mo_link_tprm_ids=tprms_with_val_type_mo_link,
                            object_type_data=linked_tmo_data,
                            objects_for_update=[linked_mo_data],
                        )
                        global_new_object_names.update(updated_objects)

                    obj_name_by_primary[mo_id].append(
                        global_new_object_names[linked_id]
                    )
            else:
                for mo_id, linked_id in mo_and_primary_parameter:
                    obj_name_by_primary[mo_id].append(linked_id)

    # GET PARENT NAMES
    for obj_ids_for_updated_chunk in get_chunked_values_by_sqlalchemy_limit(
        object_ids_need_to_be_updated
    ):
        if not object_type_data.global_uniqueness and object_type_data.p_id:
            conditions = [MO.id.in_(obj_ids_for_updated_chunk), MO.p_id != None]  # noqa
            child_and_parent_ids = session.exec(
                select(MO.id, MO.p_id).where(*conditions)
            ).all()
            if child_and_parent_ids:
                parent_ids = {
                    parent_id for _, parent_id in child_and_parent_ids
                }
                parent_id_and_name = {}
                for parent_ids_chunk in get_chunked_values_by_sqlalchemy_limit(
                    values=parent_ids
                ):
                    parent_names = session.exec(
                        select(MO.id, MO.name).where(
                            MO.id.in_(parent_ids_chunk)
                        )
                    ).all()
                    for parent_id, parent_name in parent_names:
                        parent_id_and_name[parent_id] = parent_name

                parent_id_and_name.update(global_new_object_names)
                for mo_id, parent_id in child_and_parent_ids:
                    mo_and_his_parent_name[mo_id] = parent_id_and_name[
                        parent_id
                    ]

    if not object_type_data.primary:
        obj_name_by_primary = {
            obj.id: str(obj.id) for obj in objects_for_update
        }
    else:
        obj_name_by_primary = {
            mo_id: NAME_DELIMITER.join(name_parts)
            for mo_id, name_parts in obj_name_by_primary.items()
            if name_parts
        }

    mo_and_his_parent_name.update(global_new_object_names)
    for obj in objects_for_update:
        if (
            obj.id in object_ids_need_to_be_updated
            and obj.id not in global_new_object_names.keys()
        ):
            new_mo_name = ""
            parent_name = mo_and_his_parent_name.get(obj.id)
            if (
                parent_name
                and not object_type_data.global_uniqueness
                and object_type_data.primary
                and object_type_data.p_id
            ):
                new_mo_name += parent_name + NAME_DELIMITER
            new_mo_name += obj_name_by_primary[obj.id]

            new_object_names_in_scope[obj.id] = new_mo_name
    return new_object_names_in_scope


TprmId: TypeAlias = int
MoId: TypeAlias = int


def get_prms_iterator(
    session: Session,
    tprm_ids: list[TprmId] | None = None,
    prm_ids: list[int] | None = None,
    mo_ids: list[MoId] | None = None,
    chunk_size: int = 1000,
) -> Iterator[PRM]:
    def chunk_iterator(
        _tprm_ids: list[TprmId] | None = None,
        _prm_ids: list[int] | None = None,
        _mo_ids: list[MoId] | None = None,
    ) -> Iterator[PRM]:
        stmt = select(PRM)
        if _prm_ids != [None]:
            stmt = stmt.where(PRM.id.in_(_prm_ids))
        if _tprm_ids != [None]:
            stmt = stmt.where(PRM.tprm_id.in_(_tprm_ids))
        if _mo_ids != [None]:
            stmt = stmt.where(PRM.mo_id.in_(_mo_ids))
        for _chunk in (
            session.execute(stmt).yield_per(chunk_size).partitions(chunk_size)
        ):
            yield _chunk

    if not tprm_ids:
        tprm_ids = [None]
    if not prm_ids:
        prm_ids = [None]
    if not mo_ids:
        mo_ids = [None]

    for tprm_ids_start in range(0, len(tprm_ids), SQLALCHEMY_LIMIT):
        tprm_ids_chunk = tprm_ids[
            tprm_ids_start : tprm_ids_start + SQLALCHEMY_LIMIT
        ]
        for prm_ids_start in range(0, len(prm_ids), SQLALCHEMY_LIMIT):
            prm_ids_chunk = prm_ids[
                prm_ids_start : prm_ids_start + SQLALCHEMY_LIMIT
            ]
            for mo_ids_start in range(0, len(mo_ids), SQLALCHEMY_LIMIT):
                mo_ids_chunk = mo_ids[
                    mo_ids_start : mo_ids_start + SQLALCHEMY_LIMIT
                ]
                for chunk in chunk_iterator(
                    _tprm_ids=tprm_ids_chunk,
                    _prm_ids=prm_ids_chunk,
                    _mo_ids=mo_ids_chunk,
                ):
                    yield chunk


def get_mos_iterator(
    session: Session,
    mo_ids: Iterable[MoId],
    chunk_size: int = 1000,
) -> Iterator[PRM]:
    def chunk_iterator(_mo_ids):
        stmt = select(MO).where(MO.in_(_mo_ids))
        for _chunk in (
            session.execute(stmt).yield_per(chunk_size).partitions(chunk_size)
        ):
            yield _chunk

    for mo_ids_start in range(0, len(mo_ids), SQLALCHEMY_LIMIT):
        mo_ids_chunk = mo_ids[mo_ids_start : mo_ids_start + SQLALCHEMY_LIMIT]
        for chunk in chunk_iterator(_mo_ids=mo_ids_chunk):
            yield chunk


def update_tprm_dict(
    tprm_dict: dict[TprmId, TPRM], tprm_ids: Iterable[int], session: Session
):
    new_tprm_ids = set(tprm_ids).difference(tprm_dict.keys())
    if not new_tprm_ids:
        return

    session.info["disable_security"] = True
    stmt = select(TPRM).where(TPRM.id.in_(new_tprm_ids))
    for item in session.execute(stmt).scalars():
        tprm_dict[item.tprm_id] = item


def convert_prm_value(value, converter):
    if value is None:
        return None

    def _convert_value(_value, _converter):
        if isinstance(_value, list):
            return [
                _convert_value(_value=i, _converter=_converter) for i in _value
            ]
        else:
            return _converter(_value)

    return _convert_value(_value=value, _converter=converter)


def get_flat_values_list(label_results: dict):
    def add_flat_values(_value, _results: list):
        if isinstance(_value, list):
            [add_flat_values(i, _results=_results) for i in _value]
        elif _value is not None:
            _results.append(_value)

    results = []
    for value in label_results.values():
        add_flat_values(_value=value, _results=results)
    return results


def replace_label_values(label_results: dict, replace_with: dict):
    def _replace_value(_value):
        if isinstance(_value, list):
            return [_replace_value(_value=i) for i in _value]
        else:
            return replace_with.get(_value, None)

    for key, value in label_results.items():
        label_results[key] = _replace_value(_value=value)


def get_labels_dict_chunk(
    label_results: dict[MoId, dict[TprmId, Any]],
    tmo: TMO,
    chunk_size: int = 1000,
) -> Iterator[dict]:
    empty_value = "NULL"
    chunk = {}
    size = 0
    for mo_id, prms in label_results.items():
        label_values = []
        for tprm_id in tmo.label:
            prm_value = prms.get(tprm_id, empty_value)
            if not isinstance(prm_value, str):
                prm_value = json.dumps(prm_value, default=str)
            label_values.append(prm_value)
        label = NAME_DELIMITER.join(label_values)
        chunk[mo_id] = label
        size += 1

        if size >= chunk_size:
            yield chunk
            size = 0
            chunk = {}

    if chunk:
        yield chunk


def update_labels_by_tmo(session: Session, tmo: TMO):
    if not tmo.label:
        return
    tprms_dict = {}
    update_tprm_dict(tprm_dict=tprms_dict, tprm_ids=tmo.label, session=session)

    label_result_lists: dict[MoId, dict[TprmId, Any]] = defaultdict(dict)
    for label_tprm_id in tmo.label:
        current_tprm = tprms_dict[label_tprm_id]
        current_label_results: dict[MoId, Any] = {}
        convert_by_val_type = functions_dicts.value_convertation_by_val_type[
            current_tprm.val_type
        ]
        for chunk_prms in get_prms_iterator(
            session=session, tprm_ids=[current_tprm.id]
        ):
            if not chunk_prms:
                continue
            for prm in chunk_prms:  # type: PRM
                current_label_results[prm.mo_id] = convert_prm_value(
                    value=prm.value, converter=convert_by_val_type
                )

        if current_tprm.val_type == "prm_link":
            # collecting all prm_ids in flat list
            prm_ids = get_flat_values_list(label_results=current_label_results)
            linked_prms_dict = {}
            for linked_prms_chunk in get_prms_iterator(
                session=session, prm_ids=prm_ids
            ):
                if not linked_prms_chunk:
                    continue
                tprm_ids = {prm.tprm_id for prm in linked_prms_chunk}
                if len(tprm_ids) != 1:
                    raise ValueError(
                        "The prm_link can only lead to parameters of the same type"
                    )
                update_tprm_dict(
                    tprm_dict=tprms_dict, tprm_ids=tprm_ids, session=session
                )
                current_tprm = tprms_dict[list(tprm_ids)[0]]
                convert_by_val_type = (
                    functions_dicts.value_convertation_by_val_type[
                        current_tprm.val_type
                    ]
                )
                for prm in linked_prms_chunk:
                    converted_value = convert_prm_value(
                        value=prm.value, converter=convert_by_val_type
                    )
                    linked_prms_dict[prm.id] = converted_value
            replace_label_values(
                label_results=current_label_results,
                replace_with=linked_prms_dict,
            )

        if current_tprm.val_type == "mo_link":
            mo_ids = get_flat_values_list(label_results=current_label_results)
            linked_mo_dict = {}
            for linked_mo_chunk in get_mos_iterator(
                session=session, mo_ids=mo_ids
            ):
                for mo in linked_mo_chunk:  # type: MO
                    linked_mo_dict[mo.id] = mo.name
            replace_label_values(
                label_results=current_label_results, replace_with=linked_mo_dict
            )

        for key, value in current_label_results.items():
            label_result_lists[key][label_tprm_id] = value

    for labels_dict_chunk in get_labels_dict_chunk(
        label_results=label_result_lists, tmo=tmo
    ):
        changed_mos_chunk = []
        for mos_chunk in get_mos_iterator(
            session=session, mo_ids=labels_dict_chunk.keys()
        ):
            for mo in mos_chunk:
                label = labels_dict_chunk[mo.id]
                mo.label = label
                changed_mos_chunk.append(mo)
        if changed_mos_chunk:
            session.add_all(changed_mos_chunk)
            session.flush()
    if label_result_lists:
        session.commit()


def update_all_children_by_mo_link_and_parents(
    session: Session, global_new_object_names: dict[int, str]
):
    already_updated_object_names = copy.copy(global_new_object_names)
    current_updated_object_ids = list(global_new_object_names.keys())

    while current_updated_object_ids:
        previous_updated_object_ids = copy.deepcopy(current_updated_object_ids)
        current_updated_object_ids = []
        child_objects = []

        # update objects, which are connected to already updated object by "P_ID" linking
        for (
            already_updated_object_ids_chunk
        ) in get_chunked_values_by_sqlalchemy_limit(
            previous_updated_object_ids
        ):
            response = session.execute(
                select(MO).where(MO.p_id.in_(already_updated_object_ids_chunk))
            ).scalars()
            child_objects.extend(response)

        if child_objects:
            objects_by_tmo = get_tmo_and_his_objects(child_objects)
            current_tmo_ids = set(objects_by_tmo.keys())
            current_tmos_data = session.execute(
                select(TMO).where(TMO.id.in_(current_tmo_ids))
            ).scalars()
            current_tmos_data: dict[int, TMO] = {
                tmo.id: tmo for tmo in current_tmos_data
            }

            for tmo_id in objects_by_tmo.keys():
                tmo_data = current_tmos_data[tmo_id]
                tprms_with_val_type_mo_link = (
                    get_tprms_which_has_mo_link_val_type_by_primary(
                        session=session, primary_tprms=tmo_data.primary
                    )
                )

                updated_objects = get_updated_object_names(
                    session=session,
                    global_new_object_names=already_updated_object_names,
                    objects_for_update=objects_by_tmo[tmo_id],
                    mo_link_tprm_ids=tprms_with_val_type_mo_link,
                    object_type_data=tmo_data,
                )
                current_updated_object_ids.extend(list(updated_objects.keys()))
                already_updated_object_names.update(updated_objects)

        # update objects, which are connected to already updated object by "MO_LINK" linking
        string_formatted_obj_ids_chunks = [
            [str(obj_id) for obj_id in chunk]
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                previous_updated_object_ids
            )
        ]
        child_objects_by_mo_link = []
        for string_formatted_obj_ids_chunk in string_formatted_obj_ids_chunks:
            conditions = [
                PRM.value.in_(string_formatted_obj_ids_chunk),
                MO.id == PRM.mo_id,
            ]
            response = session.execute(
                select(MO).join(PRM).where(*conditions)
            ).scalars()
            child_objects_by_mo_link.extend(response)

        if child_objects_by_mo_link:
            tmo_and_his_objects = get_tmo_and_his_objects(
                child_objects_by_mo_link
            )
            objects_by_tmo = session.execute(
                select(TMO).where(TMO.id.in_(list(tmo_and_his_objects.keys())))
            ).scalars()
            for tmo in objects_by_tmo:
                tprms_with_val_type_mo_link = session.execute(
                    select(TPRM.id).where(
                        TPRM.id.in_(tmo.primary), TPRM.val_type == "mo_link"
                    )
                ).scalars()
                tprms_with_val_type_mo_link = [
                    tprm_id for tprm_id in tprms_with_val_type_mo_link
                ]

                updated_objects = get_updated_object_names(
                    session=session,
                    global_new_object_names=already_updated_object_names,
                    objects_for_update=tmo_and_his_objects[tmo.id],
                    mo_link_tprm_ids=tprms_with_val_type_mo_link,
                    object_type_data=tmo,
                )
                current_updated_object_ids.extend(list(updated_objects.keys()))
                already_updated_object_names.update(updated_objects)

    return already_updated_object_names


def get_tmo_and_his_objects(objects: list[MO]) -> dict[int : list[MO]]:
    """
    This method get list of objects, and will return a pair of tmo_id and his objects
    """
    tmo_and_objects = {}
    for obj in objects:
        if tmo_and_objects.get(obj.tmo_id):
            tmo_and_objects[obj.tmo_id].append(obj)
            continue
        tmo_and_objects[obj.tmo_id] = [obj]
    return tmo_and_objects


def get_tprms_which_has_mo_link_val_type_by_primary(
    session: Session, primary_tprms: list[int]
) -> list[int | None]:
    """
    This method returns list of ids of TPRMs with val_type 'mo_link', which also inside primary
    """
    conditions = [TPRM.id.in_(primary_tprms), TPRM.val_type == "mo_link"]
    tprms_with_val_type_mo_link = (
        session.execute(select(TPRM.id).where(*conditions)).scalars().all()
    )
    return tprms_with_val_type_mo_link


def get_prm_and_his_data_for_single_values(
    params: list[PRM],
) -> dict[PRM.value : list[PRM]]:
    value_and_params = {}
    for param in params:
        if value_and_params.get(param.value):
            value_and_params[param.value].append(param)
            continue
        value_and_params[param.value] = [param]
    return value_and_params


def get_prm_and_his_data_for_multiple_values(
    params: list[PRM],
) -> dict[PRM.value : list[PRM]]:
    values_and_parameters = {}
    for parameter in params:
        list_of_values = utils.decode_multiple_value(parameter.value)
        for value in list_of_values:
            if values_and_parameters.get(value):
                values_and_parameters[value].append(parameter)
                continue
            values_and_parameters[value] = [parameter]
    return values_and_parameters


def delete_prm_links_which_linked_to_already_deleted_params(
    session: Session,
    already_deleted_param_ids: set,
    tprms_of_linked_params: set,
):
    for prm_ids_partition in get_chunked_values_by_sqlalchemy_limit(
        values=already_deleted_param_ids
    ):
        conditions = [
            PRM.value.in_(prm_ids_partition),
            TPRM.val_type == "prm_link",
            TPRM.multiple.is_(False),
            TPRM.constraint.in_(tprms_of_linked_params),
        ]
        stmt = select(PRM).join(TPRM).where(*conditions)
        for prm_link in session.execute(stmt).scalars().all():
            session.delete(prm_link)

    stmt = (
        select(PRM)
        .join(TPRM)
        .where(
            TPRM.val_type == "prm_link",
            TPRM.multiple.is_(True),
            TPRM.constraint.in_(tprms_of_linked_params),
        )
    )
    prm_links_to_delete = session.execute(stmt).scalars().all()

    for prm_link in prm_links_to_delete:
        multiple_value = utils.decode_multiple_value(prm_link.value)
        if multiple_value:
            multiple_value = [
                prm_link_id
                for prm_link_id in multiple_value
                if str(prm_link_id) not in already_deleted_param_ids
            ]
            if multiple_value:
                prm_link.value = pickle.dumps(multiple_value).hex()
                session.add(prm_link)
                continue
            session.delete(prm_link)


def get_linked_mo_name_for_prm_link(session: Session, parameter: PRM):
    if parameter.tprm.val_type == "mo_link":
        if parameter.tprm.multiple:
            linked_mo_names = {}

            for chunk in get_chunked_values_by_sqlalchemy_limit(
                decode_pickle_data(parameter.value)
            ):
                temp = session.exec(
                    select(MO.id, MO.name).where(MO.id.in_(chunk))
                ).all()
                linked_mo_names.update(
                    {mo_id: mo_name for mo_id, mo_name in temp}
                )

            new_value = [
                linked_mo_names[mo_id]
                for mo_id in decode_pickle_data(parameter.value)
            ]
        else:
            new_value = session.get(MO, int(parameter.value)).name

    else:
        new_value = parameter.value

    return new_value


def check_mo_is_part_of_other_mo_name(
    session: Session, object_instance_ids: set[int]
):
    tprms_with_val_type_mo_link = (
        session.execute(select(TPRM.id).where(TPRM.val_type == "mo_link"))
        .scalars()
        .all()
    )

    if tprms_with_val_type_mo_link:
        tprms_json = json.dumps(tprms_with_val_type_mo_link)

        tprm_mo_link = text("""
            SELECT id FROM tmo WHERE tmo.primary::jsonb @> :tprms_with_val_type_mo_link
        """).bindparams(
            bindparam("tprms_with_val_type_mo_link", value=tprms_json)
        )

        tprms_with_val_type_mo_link_in_primary = (
            session.execute(tprm_mo_link).scalars().all()
        )
        if tprms_with_val_type_mo_link_in_primary:
            string_mo_ids = [str(mo_id) for mo_id in object_instance_ids]

            stmt = select(cast(PRM.value, Integer)).where(
                PRM.tprm_id.in_(tprms_with_val_type_mo_link),
                PRM.value.in_(string_mo_ids),
            )
            linked_mos = session.execute(stmt).scalars().all()
            if linked_mos:
                raise HTTPException(
                    detail=f"Objects with id: {linked_mos} can't be deleted, because their names "
                    f"by primary are part of other object names",
                    status_code=422,
                )


class GetAllChildrenForObject:
    def __init__(
        self, object_id: int, session: Session, nodes_limit: int = 100
    ):
        self._main_object_id = object_id
        self._session = session
        self.NODES_LIMIT = nodes_limit
        self.__main_object_instance = self._session.get(
            MO, self._main_object_id
        )

    def __build_tree(
        self, nodes: list[dict], parent_id: Optional[int] = None
    ) -> list[dict]:
        tree = []
        for node in nodes:
            if node["parent_id"] == parent_id:
                children = self.__build_tree(nodes, node["object_id"])
                node["children"] = children
                tree.append(node)
        return tree

    def check(self) -> None:
        if self.__main_object_instance:
            return
        raise ObjectNotExists(
            status_code=422,
            detail=f"Object with id {self._main_object_id} does not exist",
        )

    def execute(self) -> dict:
        query = """
        WITH RECURSIVE descendants AS (
            SELECT
                mo.id AS object_id,
                mo.p_id AS parent_id,
                mo.tmo_id AS object_type_id,
                mo.name AS object_name
            FROM MO
            WHERE id = :start_mo_id

            UNION ALL

            SELECT
                mo.id AS object_id,
                mo.p_id AS parent_id,
                mo.tmo_id AS object_type_id,
                mo.name AS object_name
            FROM MO mo
            INNER JOIN descendants d ON mo.p_id = d.object_id
        )
        SELECT * FROM descendants;
        """

        params_for_query = {"start_mo_id": self._main_object_id}

        nodes = [
            dict(row)
            for row in self._session.execute(
                statement=text(query), params=params_for_query
            ).mappings()
        ]

        if len(nodes) > self.NODES_LIMIT:
            raise DescendantsLimit(
                status_code=422,
                detail=f"Result exceeds the limit of {self.NODES_LIMIT} descendants",
            )

        tree = self.__build_tree(
            nodes=nodes, parent_id=self.__main_object_instance.p_id
        )
        return tree[0] if tree else {}


class ObjectDBGetter:
    def __init__(self, session: Session):
        self._session = session
        self._object_type_db_getter = ObjectTypeDBGetter(session=session)

    def _get_object_instance_by_id(self, object_id: int) -> MO | None:
        query = select(MO).where(MO.id == object_id)
        object_instance = self._session.execute(query).scalar()

        if object_instance:
            return object_instance

        raise ObjectNotExists(
            status_code=422, detail=f"Object with id {object_id} not found."
        )

    def _get_object_route(self, db_object: MO, route_list: list) -> list:
        if len(db_object.children) > 0:
            for child in db_object.children:
                self._get_object_route(child, route_list)
        else:
            if db_object.point_a is not None and db_object.point_b is not None:
                route_list.append(
                    [
                        [
                            db_object.point_a.latitude,
                            db_object.point_a.longitude,
                        ],
                        [
                            db_object.point_b.latitude,
                            db_object.point_b.longitude,
                        ],
                    ]
                )
        return route_list
