import base64
import io
import math
import pickle
from typing import Generator
from typing import List, Iterable

import pandas as pd
import xlsxwriter
from fastapi import HTTPException
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import aliased
from sqlmodel import select, Session

from functions.db_functions.db_read import get_db_object_type_or_exception
from functions.functions_dicts import (
    param_type_constraint_validation,
    value_convertation_by_val_type,
)
from functions.functions_utils.utils import decode_multiple_value
from functions.validation_functions.validation_function import (
    val_type_validation_when_create_param_type,
)
from models import TPRM, MO, PRM
from routers.batch_router.exceptions import BatchCustomException
from routers.batch_router.processors import BatchImportCreator
from routers.batch_router.schemas import ExportFileTypes
from routers.parameter_router.utils import validate_param_type_if_required
from routers.parameter_type_router.schemas import TPRMCreate
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)


def check_tmo_has_sevrirty(tmo_id: int, session):
    stmt = select(TPRM).where(
        TPRM.tmo_id == tmo_id, TPRM.name.ilike("%" + "severity" + "%")
    )
    tprm = session.exec(stmt).first()
    if tprm:
        return tprm.id
    return False


def get_smallest_severity_value(severities: dict):
    smallest_value = None
    smallest_severity_name = None

    for gradation, values in severities.items():
        if "max" in values:
            current_max = values["max"]
            if current_max > 0 and (
                smallest_value is None or current_max < smallest_value
            ):
                smallest_value = current_max
                smallest_severity_name = gradation

        if "min" in values:
            current_min = values["min"]
            if current_min > 0 and (
                smallest_value is None or current_min < smallest_value
            ):
                smallest_value = current_min
                smallest_severity_name = gradation
    return smallest_severity_name, smallest_value


def get_severity_names_with_mos_quantity(
    severity_values: list, severities: dict, result: dict
):
    for value in severity_values:
        for condition, limits in severities.items():
            if "min" in limits and int(value) <= int(limits["min"]):
                continue
            if "max" in limits and int(value) > int(limits["max"]):
                continue
            result[condition] += 1

    response = {}
    for condition, count in result.items():
        response[condition] = count
    return response


def batch_import(session: Session, file_data: bytes, tmo_id: int):
    try:
        task = BatchImportCreator(
            file=file_data,
            session=session,
            tmo_id=tmo_id,
            column_name_mapping={},
            delimiter=";",
            check=False,
        )

        response = task.execute()

        return response

    except BatchCustomException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))


def create_tprm(session: Session, param_type: TPRMCreate):
    session.info["disable_security"] = True
    val_type_validation_when_create_param_type(param_type)

    if param_type.constraint:
        param_type_constraint_validation[param_type.val_type](
            param_type, session
        )
    elif param_type.val_type in ["prm_link", "formula"]:
        raise HTTPException(
            status_code=422,
            detail=f"Please, pass the constraint parameter."
            f" It is required for {param_type.val_type} val_type.",
        )

    if param_type.required:
        field_value = validate_param_type_if_required(
            session=session, param_type=param_type
        )

    param_type = param_type.dict()
    param_type["created_by"] = get_username_from_session(session=session)
    param_type["modified_by"] = get_username_from_session(session=session)
    if param_type["val_type"] != "prm_link":
        param_type["prm_link_filter"] = None

    db_param_type = TPRM(**param_type)
    session.add(db_param_type)
    session.flush([db_param_type])
    session.refresh(db_param_type)

    if db_param_type.required:
        db_objects = session.exec(
            select(MO).where(MO.tmo_id == db_param_type.tmo_id)
        ).all()
        for db_object in db_objects:
            param = PRM(
                tprm_id=db_param_type.id, mo_id=db_object.id, value=field_value
            )
            session.add(param)
            session.flush([param])
            session.refresh(param)
    session.flush([db_param_type])
    session.refresh(db_param_type)
    return db_param_type.id


def batch_export(session: Session, tmo_id: int, file_type: str):
    cache_column_name_type = {}

    get_db_object_type_or_exception(session=session, object_type_id=tmo_id)

    main_df = pd.DataFrame()

    # Adding mo parameters into df
    prm_type_where_condition = []
    # if prm_type_ids is not None:
    #     prm_type_where_condition.append(TPRM.id.in_(prm_type_ids))

    stmt = (
        select(TPRM)
        .where(TPRM.tmo_id == tmo_id, *prm_type_where_condition)
        .order_by(TPRM.group)
    )
    all_tprms = session.exec(stmt).all()

    groups_and_tprm_count = dict()

    for tprm in all_tprms:
        tprm_group_name = tprm.group if tprm.group is not None else ""

        if groups_and_tprm_count.get(tprm_group_name, None) is None:
            groups_and_tprm_count[tprm_group_name] = 0

        select_statement = select(
            PRM.mo_id.label("mo_id"), PRM.value.label("value")
        ).where(PRM.tprm_id == tprm.id)

        aliased_table = aliased(select_statement.subquery())

        stmt = (
            select(MO.id.label("mo_id"), aliased_table)
            .outerjoin(aliased_table, MO.id == aliased_table.c.mo_id)
            .where(MO.tmo_id == tmo_id)
            .order_by(MO.id)
        )

        prm_values = session.execute(stmt).all()

        result = []
        covert_by_val_type = value_convertation_by_val_type[tprm.val_type]

        if tprm.multiple:
            for prm_data in prm_values:
                if prm_data.value is not None:
                    value = decode_multiple_value(prm_data.value)
                    result.append(str(value))
                else:
                    result.append("")
        else:
            for prm_data in prm_values:
                if prm_data.value is not None:
                    result.append(covert_by_val_type(prm_data.value))
                else:
                    result.append("")
        main_df = main_df.assign(**{tprm.name: result})
        if not tprm.multiple:
            cache_column_name_type[tprm.name] = tprm.val_type

        groups_and_tprm_count[tprm_group_name] += 1

    # add parent names if parent
    aliased_table = aliased(select(MO).subquery())
    stmt = (
        select(
            MO.id.label("mo_id"),
            MO.name,
            aliased_table.c.name.label("parent_name"),
        )
        .outerjoin(aliased_table, MO.p_id == aliased_table.c.id)
        .where(MO.tmo_id == tmo_id)
        .order_by(MO.id)
    )

    res = session.execute(stmt).all()
    temporary_df = pd.DataFrame(
        data=res, columns=["mo_id", "name", "parent_name"]
    )
    main_df["parent_name"] = temporary_df["parent_name"]
    main_df["mo_id"] = temporary_df["mo_id"]
    main_df["name"] = temporary_df["name"]
    main_df = main_df.convert_dtypes()

    if file_type == ExportFileTypes.csv.value:
        output = io.BytesIO()
        main_df.to_csv(path_or_buf=output, index=False, sep=";")
        output.seek(0)

    else:
        start_row_for_df = 0
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        cell_format = workbook.add_format(
            {
                "bold": True,
                "font_color": "black",
                "align": "center",
                "valign": "center",
                "border": 1,
                "border_color": "black",
            }
        )
        worksheet = workbook.add_worksheet("sheet1")

        # Add group headers
        if start_row_for_df == 1:
            first_column = 0
            for group_name, column_count in groups_and_tprm_count.items():
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
        for column in main_df:
            worksheet.write(row_number, column_number, column, cell_format)
            row_number += 1

            cell_format = None
            column_type = cache_column_name_type.get(column)
            if column_type == "bool":
                cell_format = workbook.add_format({"num_format": "BOOLEAN"})

            worksheet.write_column(
                row_number,
                column_number,
                list(main_df[column].values),
                cell_format,
            )
            column_number += 1
            row_number = start_row_for_df
        workbook.close()
        output.seek(0)

    output = base64.b64encode(output.read()).decode("utf-8")
    for i in range(0, len(output), 16384):
        yield output[i : i + 16384]


class GetAllMOWithParamsByTMOId:
    MAX_COUNT_OF_TPRMS_IN_STEP = 300
    MO_COUNT_PER_QUERY = 15_000  # 30_000

    def __init__(self, session: Session, tmo_id: int):
        self.session = session
        self.tmo_id = tmo_id
        self._tprm_cache = None
        self._list_of_tprms_per_step = None
        self._list_of_statements = None

    @property
    def tmo_id(self):
        return self._tmo_id

    @tmo_id.setter
    def tmo_id(self, value: int):
        self._tmo_id = value

    def __load_necessary_data(self):
        # get tprm cache
        self._tprm_cache = self.get_all_tprms_cache()

        # find list of tprms per step
        self._list_of_tprms_per_step = self.get_list_of_tprms_per_step()
        # get list of statements
        self._list_of_statements = self.get_list_of_statements()

    def get_all_tprms_cache(self):
        """Return dict with str(tprm.id) as key and tprm data as value"""
        stmt = select(TPRM).where(TPRM.tmo_id == self.tmo_id)
        tprms_cache = self.session.exec(stmt).all()
        tprms_cache = {
            str(item.id): {
                "multiple": item.multiple,
                "val_type": item.val_type,
                "tprm_id": item.id,
            }
            for item in tprms_cache
        }
        return tprms_cache

    def get_list_of_tprms_per_step(self) -> List[List[str]]:
        """Returns list of lists with tprms ids as str type."""
        count_of_steps = math.ceil(
            len(self._tprm_cache.keys()) / self.MAX_COUNT_OF_TPRMS_IN_STEP
        )

        tprm_ids_as_str = list(self._tprm_cache.keys())
        list_of_tprms_per_step = []
        for step_number in range(count_of_steps):
            start = step_number * self.MAX_COUNT_OF_TPRMS_IN_STEP
            end = start + self.MAX_COUNT_OF_TPRMS_IN_STEP
            step = tprm_ids_as_str[start:end]
            if step:
                list_of_tprms_per_step.append(tprm_ids_as_str[start:end])
        return list_of_tprms_per_step

    def get_list_of_statements(self):
        """Returns lists of query statements"""
        list_of_statements = []
        if not self._tprm_cache:
            stmt = select(MO).where(MO.tmo_id == self.tmo_id)
            list_of_statements.append(stmt)
        else:
            for tprms_in_one_step in self._list_of_tprms_per_step:
                if tprms_in_one_step:
                    stmt = select(MO).where(MO.tmo_id == self.tmo_id)
                    for tprm_id_as_str in tprms_in_one_step:
                        label_name = tprm_id_as_str
                        data = self._tprm_cache[tprm_id_as_str]
                        select_stmt = select(
                            PRM.mo_id, PRM.value.label(label_name)
                        ).where(PRM.tprm_id == data["tprm_id"])
                        aliased_table = aliased(select_stmt.subquery())
                        stmt = stmt.join(
                            aliased_table,
                            MO.id == aliased_table.c.mo_id,
                            isouter=True,
                        ).add_columns(
                            getattr(aliased_table.c, label_name, None)
                        )
                    list_of_statements.append(stmt)

        return list_of_statements

    @staticmethod
    def get_set_of_item_params(item: RowMapping) -> set[str]:
        """Returns set of params ids as set of str"""
        tprms_keys = set(item.keys())
        tprms_keys.remove("MO")
        return tprms_keys

    def get_params_as_dict_from_row_item(
        self, item: RowMapping, params_ids_as_str: Iterable[str]
    ) -> List[dict]:
        """Returns dict of params ids as keys and their converter by type values for particular item"""
        params = []
        for tprm_key in params_ids_as_str:
            # tprm_value = getattr(item, tprm_key, None)
            tprm_value = item.get(tprm_key, None)

            if tprm_value is None:
                continue

            # check on empty string
            tprm_data = self._tprm_cache.get(tprm_key)
            if not tprm_data:
                continue
            if not tprm_value and tprm_data["val_type"] != "str":
                continue
            params.append(
                {
                    "tprm_id": tprm_key,
                    "value": self.get_tprm_value(
                        tprm_value, tprm_data["val_type"], tprm_data["multiple"]
                    ),
                }
            )

        return params

    @staticmethod
    def get_tprm_value(tprm_value: str, tprm_val_type: str, tprm_multiple):
        if tprm_multiple:
            return decode_multiple_value(tprm_value)
        else:
            convert_func = value_convertation_by_val_type.get(tprm_val_type)
            if convert_func:
                return convert_func(tprm_value)
            else:
                return tprm_value

    def get_result_generator(self, replace_links: bool = False) -> Generator:
        self.__load_necessary_data()

        # get_all_mo_ids
        stmt = select(MO.id).where(MO.tmo_id == self.tmo_id)
        all_mo_ids = self.session.execute(stmt).scalars().all()
        print(f"all_mo_len: {len(all_mo_ids)}")
        count_of_steps = math.ceil(len(all_mo_ids) / self.MO_COUNT_PER_QUERY)

        for step in range(count_of_steps):
            start = step * self.MO_COUNT_PER_QUERY
            end = start + self.MO_COUNT_PER_QUERY
            mo_ids_for_step = all_mo_ids[start:end]

            step_data = []

            select_statement = select(
                MO.id.label("id"), MO.name.label("parent_name")
            )
            aliased_table = aliased(select_statement.subquery())

            stmt_point_a = select(
                MO.id.label("id"), MO.name.label("point_a_name")
            )
            al_table_point_a = aliased(stmt_point_a.subquery())

            stmt_point_b = select(
                MO.id.label("id"), MO.name.label("point_b_name")
            )
            al_table_point_b = aliased(stmt_point_b.subquery())

            for stmt in self._list_of_statements:
                exec_stmt = (
                    stmt.where(MO.id.in_(mo_ids_for_step))
                    .outerjoin(aliased_table, MO.p_id == aliased_table.c.id)
                    .outerjoin(
                        al_table_point_a, MO.point_a_id == al_table_point_a.c.id
                    )
                    .outerjoin(
                        al_table_point_b, MO.point_b_id == al_table_point_b.c.id
                    )
                    .add_columns(
                        aliased_table.c.parent_name,
                        al_table_point_a.c.point_a_name,
                        al_table_point_b.c.point_b_name,
                    )
                )
                res = [
                    item._asdict()
                    for item in self.session.execute(exec_stmt).fetchall()
                ]

                if res:
                    step_data.append(res)

            if len(step_data) > 0:
                base_objects_tprms = self.get_set_of_item_params(
                    step_data[0][0]
                )
                base_objects_data = dict()

                for mo_data_row in step_data[0]:
                    mo_as_dict = mo_data_row["MO"].dict()
                    mo_as_dict["parent_name"] = mo_data_row.get("parent_name")
                    mo_as_dict["point_a_name"] = mo_data_row.get("point_a_name")
                    mo_as_dict["point_b_name"] = mo_data_row.get("point_b_name")
                    if replace_links:
                        mo_data_row = self.replace_mo_links_in_data_row(
                            base_objects_tprms, mo_data_row
                        )
                        mo_data_row = self.replace_prm_links_in_data_row(
                            base_objects_tprms, mo_data_row
                        )
                    mo_as_dict["params"] = (
                        self.get_params_as_dict_from_row_item(
                            item=mo_data_row,
                            params_ids_as_str=base_objects_tprms,
                        )
                    )

                    base_objects_data[mo_data_row["MO"].id] = mo_as_dict

                for mo_data_rows in step_data[1:]:
                    objects_tprms = self.get_set_of_item_params(mo_data_rows[0])

                    for mo_data_row in mo_data_rows:
                        mo_from_base = base_objects_data.get(
                            mo_data_row["MO"].id
                        )
                        if mo_from_base:
                            if replace_links:
                                mo_data_row = self.replace_mo_links_in_data_row(
                                    base_objects_tprms, mo_data_row
                                )
                                # mo_data_row = self.replace_prm_links_in_data_row(base_objects_tprms, mo_data_row)
                            params = self.get_params_as_dict_from_row_item(
                                item=mo_data_row,
                                params_ids_as_str=objects_tprms,
                            )
                            mo_from_base["params"].extend(params)

                base_objects_data = [
                    pickle.dumps(item).hex()
                    for item in base_objects_data.values()
                ]
                print(f"yielding {len(base_objects_data)} objects")
                yield base_objects_data

    def replace_mo_links_in_data_row(
        self, tprms: list | set, mo_data_row: dict
    ):
        links = []
        for tprm in tprms:
            if self._tprm_cache.get(tprm):
                if self._tprm_cache.get(tprm).get("val_type") == "mo_link":
                    links.append(tprm)
        link_values = [
            int(mo_data_row[link])
            for link in links
            if mo_data_row.get(link) is not None
        ]
        query = select(MO.id, MO.name).where(MO.id.in_(link_values))
        link_values = self.session.execute(query)
        link_values = {str(lv[0]): lv[1] for lv in link_values}
        for k in mo_data_row.keys():
            if k in links and mo_data_row[k]:
                mo_data_row[k] = link_values[mo_data_row[k]]
        return mo_data_row

    def replace_prm_links_in_data_row(
        self, tprms: list | set, mo_data_row: dict
    ):
        links = []
        for tprm in tprms:
            if self._tprm_cache.get(tprm):
                if self._tprm_cache.get(tprm).get("val_type") == "prm_link":
                    links.append(tprm)
        link_values = [
            mo_data_row[link]
            for link in links
            if mo_data_row.get(link) is not None
        ]
        query = select(PRM.id, PRM.value).where(PRM.id.in_(link_values))
        link_values = self.session.execute(query)
        link_values = {str(lv[0]): lv[1] for lv in link_values}
        for k in mo_data_row.keys():
            if k in links and mo_data_row[k]:
                mo_data_row[k] = link_values[mo_data_row[k]]
        return mo_data_row


class GetAllMOAttrsByTMOIdWithSpecialParameters:
    MAX_COUNT_OF_TPRMS_IN_STEP = 300
    MO_COUNT_PER_QUERY = 1000

    def __init__(
        self, session: Session, tmo_id: int, tprm_ids: List[int] = None
    ):
        self.session = session
        self.tmo_id = tmo_id
        self._tprm_cache = None
        self._list_of_tprms_per_step = None
        self._list_of_statements = None
        self.tprm_ids = tprm_ids

    @property
    def tmo_id(self):
        return self._tmo_id

    @tmo_id.setter
    def tmo_id(self, value: int):
        self._tmo_id = value

    def __load_necessary_data(self):
        # get tprm cache
        self._tprm_cache = self.get_all_tprms_cache()

        # find list of tprms per step
        self._list_of_tprms_per_step = self.get_list_of_tprms_per_step()
        # get list of statements
        self._list_of_statements = self.get_list_of_statements()

    def get_all_tprms_cache(self):
        """Return dict with str(tprm.id) as key and tprm data as value"""
        if self.tprm_ids is not None:
            stmt = select(TPRM).where(
                TPRM.tmo_id == self.tmo_id, TPRM.id.in_(self.tprm_ids)
            )
        else:
            stmt = select(TPRM).where(TPRM.tmo_id == self.tmo_id)

        tprms_cache = self.session.exec(stmt).all()
        tprms_cache = {
            str(item.id): {
                "multiple": item.multiple,
                "val_type": item.val_type,
                "tprm_id": item.id,
            }
            for item in tprms_cache
        }
        print("TPRM CACHE")
        print(tprms_cache)
        return tprms_cache

    def get_list_of_tprms_per_step(self) -> List[List[str]]:
        """Returns list of lists with tprms ids as str type."""
        count_of_steps = math.ceil(
            len(self._tprm_cache.keys()) / self.MAX_COUNT_OF_TPRMS_IN_STEP
        )

        tprm_ids_as_str = list(self._tprm_cache.keys())
        list_of_tprms_per_step = []
        for step_number in range(count_of_steps):
            start = step_number * self.MAX_COUNT_OF_TPRMS_IN_STEP
            end = start + self.MAX_COUNT_OF_TPRMS_IN_STEP
            step = tprm_ids_as_str[start:end]
            if step:
                list_of_tprms_per_step.append(tprm_ids_as_str[start:end])
        return list_of_tprms_per_step

    def get_list_of_statements(self):
        """Returns lists of query statements"""
        list_of_statements = []
        if not self._tprm_cache:
            stmt = select(MO).where(MO.tmo_id == self.tmo_id)
            list_of_statements.append(stmt)
        else:
            for tprms_in_one_step in self._list_of_tprms_per_step:
                if tprms_in_one_step:
                    stmt = select(MO).where(MO.tmo_id == self.tmo_id)
                    for tprm_id_as_str in tprms_in_one_step:
                        label_name = tprm_id_as_str
                        data = self._tprm_cache[tprm_id_as_str]
                        select_stmt = select(
                            PRM.mo_id, PRM.value.label(label_name)
                        ).where(PRM.tprm_id == data["tprm_id"])
                        aliased_table = aliased(select_stmt.subquery())
                        stmt = stmt.join(
                            aliased_table,
                            MO.id == aliased_table.c.mo_id,
                            isouter=True,
                        ).add_columns(
                            getattr(aliased_table.c, label_name, None)
                        )
                    list_of_statements.append(stmt)

        return list_of_statements

    @staticmethod
    def get_set_of_item_params(item: RowMapping) -> set[str]:
        """Returns set of params ids as set of str"""
        tprms_keys = set(item.keys())
        tprms_keys.remove("MO")
        return tprms_keys

    def get_params_as_dict_from_row_item(
        self, item: RowMapping, params_ids_as_str: Iterable[str]
    ) -> List[dict]:
        """Returns dict of params ids as keys and their converter by type values for particular item"""
        params = dict()
        for tprm_key in params_ids_as_str:
            tprm_value = getattr(item, tprm_key, None)

            if tprm_value is None:
                continue

            # check on empty string
            tprm_data = self._tprm_cache.get(tprm_key)
            if not tprm_data:
                continue
            if not tprm_value and tprm_data["val_type"] != "str":
                continue
            params[tprm_key] = self.get_tprm_value(
                tprm_value, tprm_data["val_type"], tprm_data["multiple"]
            )

        return params

    @staticmethod
    def get_tprm_value(tprm_value: str, tprm_val_type: str, tprm_multiple):
        if tprm_multiple:
            return decode_multiple_value(tprm_value)
        else:
            convert_func = value_convertation_by_val_type.get(tprm_val_type)
            if convert_func:
                return convert_func(tprm_value)
            else:
                return tprm_value

    def get_result_generator(self) -> Generator:
        self.__load_necessary_data()

        # get_all_mo_ids
        # Do not change the sort order. The correctness of the hierarchy depends on it
        stmt = select(MO.id).where(MO.tmo_id == self.tmo_id).order_by(MO.p_id)
        all_mo_ids = self.session.execute(stmt).scalars().all()
        count_of_steps = math.ceil(len(all_mo_ids) / self.MO_COUNT_PER_QUERY)

        for step in range(count_of_steps):
            start = step * self.MO_COUNT_PER_QUERY
            end = start + self.MO_COUNT_PER_QUERY
            mo_ids_for_step = all_mo_ids[start:end]

            step_data = []

            for stmt in self._list_of_statements:
                exec_stmt = stmt.where(MO.id.in_(mo_ids_for_step))
                res = self.session.execute(exec_stmt).mappings().all()
                if res:
                    step_data.append(res)

            if len(step_data) > 0:
                base_objects_tprms = self.get_set_of_item_params(
                    step_data[0][0]
                )
                base_objects_data = dict()

                for mo_data_row in step_data[0]:
                    mo_as_dict = mo_data_row.MO.dict()

                    params_as_dict = self.get_params_as_dict_from_row_item(
                        item=mo_data_row, params_ids_as_str=base_objects_tprms
                    )
                    mo_as_dict.update(params_as_dict)

                    base_objects_data[mo_data_row.MO.id] = mo_as_dict

                for mo_data_rows in step_data[1:]:
                    objects_tprms = self.get_set_of_item_params(mo_data_rows[0])

                    for mo_data_row in mo_data_rows:
                        mo_from_base = base_objects_data.get(mo_data_row.MO.id)
                        if mo_from_base:
                            params_as_dict = (
                                self.get_params_as_dict_from_row_item(
                                    item=mo_data_row,
                                    params_ids_as_str=objects_tprms,
                                )
                            )
                            mo_from_base.update(params_as_dict)

                base_objects_data = [
                    pickle.dumps(item).hex()
                    for item in base_objects_data.values()
                ]
                yield base_objects_data
