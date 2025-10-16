import io
import json
from typing import Any

import numpy as np
import pandas as pd
import xlsxwriter
from fastapi import UploadFile
from pandas import DataFrame
from sqlalchemy.orm import Session
from sqlmodel import select
from xlsxwriter.exceptions import DuplicateWorksheetName
from xlsxwriter.worksheet import Worksheet

from models import TMO, TPRM
from routers.batch_router.constants import XLSX_FORMAT
from routers.migration_router.constants import (
    SEPARATOR_FOR_PRM_LINK,
    CONVERT_TO_STRING_SYMBOL,
)
from routers.migration_router.exceptions import (
    NotValidFileContentType,
    ObjectTypeAlreadyExists,
)
from routers.migration_router.schemas import (
    MigrateObjectTypeAsExportRequest,
    ParsedRequestedFileAsDict,
)
from routers.migration_router.utils import (
    get_all_parent_object_type_ids,
    MigrateObjectTypeUtils,
    get_all_child_object_type_ids,
)
from routers.object_type_router.exceptions import ObjectTypeNotExists
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)
from val_types.constants import two_way_mo_link_val_type_name


class ConvertObjectTypeServiceData:
    COLUMNS_WITH_TPRM_IDS = [
        "latitude",
        "longitude",
        "status",
        "severity_id",
        "label",
        "primary",
    ]
    COLUMNS_WITH_TMO_IDS = ["p_id", "points_constraint_by_tmo"]

    def __init__(self, session: Session, dataframe: pd.DataFrame):
        self._session = session
        self._dataframe = dataframe

    def _validate_columns(self) -> None:
        missing_tprm = [
            col
            for col in self.COLUMNS_WITH_TPRM_IDS
            if col not in self._dataframe.columns
        ]
        missing_tmo = [
            column_name
            for column_name in self.COLUMNS_WITH_TMO_IDS
            if column_name not in self._dataframe.columns
        ]
        if missing_tprm or missing_tmo:
            raise ValueError(
                f"Missing columns: TPRM {missing_tprm}, TMO {missing_tmo}"
            )

    def _get_unique_ids(self, columns: list) -> set:
        columns_data = self._dataframe[columns].stack().explode()
        clear_ids = (
            columns_data[columns_data.notnull() & (columns_data != "")]
            .dropna()
            .astype(int)
        )

        return set(clear_ids.tolist())

    def get_instance_name_by_id(
        self, id_field: Any, name_field: Any, requested_ids: set
    ) -> dict:
        if not requested_ids:
            return {}

        query = select(id_field, name_field).where(id_field.in_(requested_ids))
        instance_data = self._session.execute(query).all()
        return {
            instance_id: instance_name
            for instance_id, instance_name in instance_data
        }

    @staticmethod
    def _convert_column_values(
        column_data: pd.Series, mapping: dict
    ) -> pd.Series:
        def convert_value(value):
            if value:
                if isinstance(value, list):
                    new_value = [mapping[value] for value in value]
                else:
                    new_value = mapping[value]

                return new_value

            return value

        return column_data.apply(convert_value)

    def process(self) -> pd.DataFrame:
        if self._dataframe.empty:
            return self._dataframe.copy()

        self._validate_columns()

        parameter_type_name_by_id = self.get_instance_name_by_id(
            id_field=TPRM.id,
            name_field=TPRM.name,
            requested_ids=self._get_unique_ids(
                columns=self.COLUMNS_WITH_TPRM_IDS
            ),
        )

        object_type_name_by_id = self.get_instance_name_by_id(
            id_field=TMO.id,
            name_field=TMO.name,
            requested_ids=self._get_unique_ids(
                columns=self.COLUMNS_WITH_TMO_IDS
            ),
        )

        new_data = self._dataframe.copy()

        for column in self.COLUMNS_WITH_TPRM_IDS:
            if parameter_type_name_by_id:
                new_data[column] = self._convert_column_values(
                    column_data=new_data[column],
                    mapping=parameter_type_name_by_id,
                )

        for column in self.COLUMNS_WITH_TMO_IDS:
            if object_type_name_by_id:
                new_data[column] = self._convert_column_values(
                    column_data=new_data[column], mapping=object_type_name_by_id
                )

        return new_data


class ConvertParameterTypeServiceData:
    def __init__(
        self,
        session: Session,
        parameter_type_instances_by_id: dict[int, TPRM],
        dataframe: pd.DataFrame,
    ):
        self._session = session
        self._parameter_type_instances_by_id = parameter_type_instances_by_id
        self._dataframe = dataframe

    @staticmethod
    def _convert_constraint_column(value, parameter_type_instance, session):
        def _convert_prm_link_constraint(constraint_value: str):
            query = (
                select(TPRM).join(TMO).where(TPRM.id == int(constraint_value))
            )
            linked_parameter_type_instance = session.execute(query).scalar()
            linked_object_type_instance = linked_parameter_type_instance.tmo
            return (
                linked_parameter_type_instance.name
                + SEPARATOR_FOR_PRM_LINK
                + linked_object_type_instance.name
            )

        def _convert_mo_link_constraint(constraint_value: str):
            return session.get(TMO, int(constraint_value)).name

        convert_constraint_method_by_val_type = {
            "prm_link": _convert_prm_link_constraint,
            "mo_link": _convert_mo_link_constraint,
            two_way_mo_link_val_type_name: _convert_mo_link_constraint,
        }

        process_method = convert_constraint_method_by_val_type.get(
            parameter_type_instance.val_type
        )
        if process_method:
            return process_method(constraint_value=value)

        return value

    @staticmethod
    def _convert_prm_link_filter_column(value, session):
        if value:
            parameter_type_ids = value.split(":")
            query = select(TPRM.id, TPRM.name).where(
                TPRM.id.in_(parameter_type_ids)
            )
            parameter_type_name_by_id = {
                parameter_type_id: parameter_type_name
                for parameter_type_id, parameter_type_name in session.execute(
                    query
                )
            }

            return CONVERT_TO_STRING_SYMBOL + ":".join(
                [
                    parameter_type_name_by_id[int(parameter_type_id)]
                    for parameter_type_id in parameter_type_ids
                ]
            )

        return value

    @staticmethod
    def _convert_backward_link_column(value, session):
        if value:
            return session.get(TPRM, int(value)).name

        return value

    def process(self):
        new_data = self._dataframe.copy()

        if "prm_link_filter" in new_data:
            new_data["prm_link_filter"] = new_data["prm_link_filter"].replace(
                [np.nan], None
            )
            new_data["prm_link_filter"] = new_data["prm_link_filter"].apply(
                self._convert_prm_link_filter_column, session=self._session
            )

        if "backward_link" in new_data:
            new_data["backward_link"] = new_data["backward_link"].replace(
                [np.nan], None
            )
            new_data["backward_link"] = new_data["backward_link"].apply(
                self._convert_backward_link_column, session=self._session
            )

        for index, row in new_data.iterrows():
            if row["constraint"]:
                new_data.loc[index, "constraint"] = (
                    self._convert_constraint_column(
                        value=row["constraint"],
                        parameter_type_instance=self._parameter_type_instances_by_id[
                            row["id"]
                        ],
                        session=self._session,
                    )
                )

        return new_data


class MigrateObjectTypeAsExport:
    def __init__(
        self, request: MigrateObjectTypeAsExportRequest, session: Session
    ):
        self._request = request
        self._session = session
        self._output = io.BytesIO()
        self._workbook = xlsxwriter.Workbook(self._output)

        self._row_id = 0

    def _get_dataframe_by_object_type(
        self, object_type_id: int
    ) -> pd.DataFrame:
        query = select(TMO).where(TMO.id == object_type_id)
        object_type_instance = self._session.execute(query).scalar()

        object_type_instance_as_dict = dict(object_type_instance)
        object_type_instance_as_dict["creation_date"] = str(
            object_type_instance_as_dict["creation_date"]
        )
        object_type_instance_as_dict["modification_date"] = str(
            object_type_instance_as_dict["modification_date"]
        )

        data_for_column = []
        columns = list(object_type_instance_as_dict.keys())

        data_for_column.append(list(object_type_instance_as_dict.values()))

        object_type_data = pd.DataFrame(data=data_for_column, columns=columns)
        task = ConvertObjectTypeServiceData(
            session=self._session,
            dataframe=object_type_data,
        )

        processed_dataframe = task.process()

        return processed_dataframe.drop(["id"], axis=1)

    def _get_dataframe_by_parameter_types(
        self, object_type_id: int
    ) -> pd.DataFrame:
        query = select(TPRM).where(TPRM.tmo_id == object_type_id)
        parameter_type_instances = self._session.execute(query).scalars().all()

        data_for_column = []
        columns = None

        parameter_type_instances_by_id = {
            parameter_type_instance.id: parameter_type_instance
            for parameter_type_instance in parameter_type_instances
        }

        for parameter_type_instance in parameter_type_instances:
            parameter_type_instance_as_dict = dict(parameter_type_instance)

            parameter_type_instance_as_dict["creation_date"] = str(
                parameter_type_instance_as_dict["creation_date"]
            )
            parameter_type_instance_as_dict["modification_date"] = str(
                parameter_type_instance_as_dict["modification_date"]
            )

            if columns is None:
                columns = list(parameter_type_instance_as_dict.keys())

            data_for_column.append(
                list(parameter_type_instance_as_dict.values())
            )

        dataframe_with_data = pd.DataFrame(
            data=data_for_column, columns=columns
        )

        task = ConvertParameterTypeServiceData(
            session=self._session,
            parameter_type_instances_by_id=parameter_type_instances_by_id,
            dataframe=dataframe_with_data,
        )
        processed_data = task.process()
        if processed_data.empty:
            return processed_data

        return processed_data.drop(["id", "tmo_id"], axis=1)

    def _write_dataframe_to_xlsx(
        self, data: pd.DataFrame, worksheet: Worksheet, title: str
    ):
        self._pass_rows(worksheet=worksheet)
        worksheet.write(f"A{self._row_id}", title)

        for col_num, column in enumerate(data.columns):
            worksheet.write(self._row_id, col_num, column)

        for row in data.itertuples(index=False, name=None):
            self._row_id += 1

            for col_num, value in enumerate(row):
                if isinstance(value, (list, dict)):
                    worksheet.write(self._row_id, col_num, json.dumps(value))
                    continue

                worksheet.write(self._row_id, col_num, value)

        self._pass_rows(worksheet=worksheet, rows_quantity=3)

    def _pass_rows(self, worksheet: Worksheet, rows_quantity: int = 1):
        for _ in range(rows_quantity):
            self._row_id += 1
            worksheet.write(f"A{self._row_id}", "")

    @staticmethod
    def _clean_sheet_name(sheet_name: str):
        symbols = ["[", "]", ":", "*", "?", "/", "\\"]
        for symbol in symbols:
            sheet_name = sheet_name.replace(symbol, "")

        return sheet_name

    def execute(self):
        query = select(TMO).where(TMO.id == self._request.object_type_id)
        requested_object_type_instance = self._session.execute(query).scalar()
        if not requested_object_type_instance:
            raise ObjectTypeNotExists(
                status_code=422,
                detail=f"Object type with id {self._request.object_type_id} does not exist",
            )
        additional_object_type_ids = []
        if self._request.parents:
            parent_object_type_ids = get_all_parent_object_type_ids(
                session=self._session,
                main_object_type_instance=requested_object_type_instance,
            )
            additional_object_type_ids.extend(parent_object_type_ids)

        if self._request.children:
            child_object_type_ids = get_all_child_object_type_ids(
                session=self._session,
                main_object_type_instance=requested_object_type_instance,
            )
            child_object_type_ids.pop(0)
            additional_object_type_ids.extend(child_object_type_ids)

        query = select(TMO).where(TMO.id.in_(additional_object_type_ids))
        additional_object_type_instances = (
            self._session.execute(query).scalars().all()
        )

        object_type_instances_to_process = [requested_object_type_instance]
        object_type_instances_to_process.extend(
            additional_object_type_instances
        )

        for object_type_instance in object_type_instances_to_process:
            self._row_id = 0
            sheet_name = self._clean_sheet_name(
                sheet_name=object_type_instance.name
            )
            try:
                worksheet = self._workbook.add_worksheet(sheet_name)
            except DuplicateWorksheetName:
                worksheet = self._workbook.add_worksheet(
                    f"{sheet_name} + {object_type_instance.id}"
                )

            object_type_data = self._get_dataframe_by_object_type(
                object_type_id=object_type_instance.id
            )
            parameter_type_data = self._get_dataframe_by_parameter_types(
                object_type_id=object_type_instance.id
            )

            self._write_dataframe_to_xlsx(
                data=object_type_data,
                worksheet=worksheet,
                title="OBJECT TYPE",
            )

            self._write_dataframe_to_xlsx(
                data=parameter_type_data,
                worksheet=worksheet,
                title="PARAMETER TYPE",
            )

        self._workbook.close()
        self._output.seek(0)
        return self._output


class ProcessFileForMigrateImport:
    OBJECT_TYPE_ATTRIBUTES_INDEX = 0
    OBJECT_TYPE_VALUES_INDEX = 1
    PARAMETER_TYPE_ATTRIBUTES_INDEX = 5
    PARAMETER_TYPE_VALUES_INDEX = 6

    def __init__(self, session: Session, file: UploadFile):
        self._session = session
        self._file_data_in_bytes = file.file.read()
        self._file_content_type = file.content_type

    def _get_dataframe_from_file_data(self) -> dict[Any, DataFrame]:
        with io.BytesIO(self._file_data_in_bytes) as data:
            if self._file_content_type != XLSX_FORMAT:
                raise NotValidFileContentType(
                    status_code=422,
                    detail=f"Content type: {self._file_content_type} is not allowed. Allowed "
                    f"content types: xlsx",
                )
            return pd.read_excel(
                data,
                engine="openpyxl",
                dtype=str,
                keep_default_na=False,
                sheet_name=None,
            )

    def _get_dataframe_data_in_dict(
        self, sheets_as_dataframes: dict[Any, DataFrame]
    ) -> ParsedRequestedFileAsDict:
        """
        Read a prepared file containing object type and parameter type data.

        This method reads all data from the file and creates a class instance
        that stores all required information for further processing.
        """
        sheet_data_by_object_type_name = {}

        for sheet_name, dataframe in sheets_as_dataframes.items():
            object_type_keys = dataframe.iloc[
                self.OBJECT_TYPE_ATTRIBUTES_INDEX
            ].tolist()
            object_type_values = dataframe.iloc[
                self.OBJECT_TYPE_VALUES_INDEX
            ].tolist()
            requested_object_type_instance = dict(
                zip(object_type_keys, object_type_values)
            )

            requested_parameter_type_instances = []

            if self.PARAMETER_TYPE_ATTRIBUTES_INDEX < len(dataframe):
                parameter_type_attributes = dataframe.iloc[
                    self.PARAMETER_TYPE_ATTRIBUTES_INDEX
                ].tolist()

                for index in range(
                    self.PARAMETER_TYPE_VALUES_INDEX, len(dataframe)
                ):
                    values = dataframe.iloc[index].tolist()
                    param_dict = {
                        attribute: value
                        for attribute, value in zip(
                            parameter_type_attributes, values
                        )
                        if attribute
                    }
                    requested_parameter_type_instances.append(param_dict)

            sheet_data_by_object_type_name[
                requested_object_type_instance["name"]
            ] = {
                "object_type": requested_object_type_instance,
                "parameter_types": requested_parameter_type_instances,
            }

        self._full_requested_data = ParsedRequestedFileAsDict(
            parameter_type_instances_by_object_type_name={
                object_type_name: sheet_data["parameter_types"]
                for object_type_name, sheet_data in sheet_data_by_object_type_name.items()
            },
            object_type_instances_by_name={
                object_type_name: sheet_data["object_type"]
                for object_type_name, sheet_data in sheet_data_by_object_type_name.items()
            },
        )
        return self._full_requested_data


class MigrateObjectTypeAsImport(ProcessFileForMigrateImport):
    def __init__(self, session: Session, file: UploadFile):
        self._session = session
        self._username = get_username_from_session(session=self._session)

        self._requested_data = ParsedRequestedFileAsDict()
        self._utils = MigrateObjectTypeUtils(session=session)
        super().__init__(
            session=session,
            file=file,
        )

    def _convert_object_type_independent_values(
        self, object_type_instance: dict
    ):
        new_object_type_instance = dict()
        convert_method_by_attribute_name = {
            "geometry_type": self._utils.convert_to_string,
            "description": self._utils.convert_to_string,
            "icon": self._utils.convert_to_string,
            "line_type": self._utils.convert_to_string,
            "name": self._utils.convert_to_string,
            "materialize": self._utils.convert_to_bool,
            "minimize": self._utils.convert_to_bool,
            "inherit_location": self._utils.convert_to_bool,
            "virtual": self._utils.convert_to_bool,
            "global_uniqueness": self._utils.convert_to_bool,
        }
        for attribute_name, value in object_type_instance.items():
            convert_method = convert_method_by_attribute_name.get(
                attribute_name
            )

            if convert_method:
                new_object_type_instance[attribute_name] = convert_method(
                    value=value
                )

        new_object_type_instance["created_by"] = self._username
        new_object_type_instance["modified_by"] = self._username
        return new_object_type_instance

    def _convert_object_type_dependent_values(
        self,
        object_type_instance: TMO,
    ):
        object_type_instance_in_dict = dict(object_type_instance)
        new_object_type_data = dict()
        convert_method_by_attribute_name = {
            "longitude": self._utils.process_parameter_type_for_object_type,
            "latitude": self._utils.process_parameter_type_for_object_type,
            "status": self._utils.process_parameter_type_for_object_type,
            "label": self._utils.process_list_parameter_types_for_object_type,
            "severity_id": self._utils.process_parameter_type_for_object_type,
            "primary": self._utils.process_list_parameter_types_for_object_type,
            "points_constraint_by_tmo": self._utils.process_list_parameter_types_for_object_type,
            "p_id": self._utils.convert_object_type_parent_id,
            "lifecycle_process_definition": self._utils.check_lifecycle_process_definition_exists,
        }
        for attribute_name, value in object_type_instance_in_dict.items():
            convert_method = convert_method_by_attribute_name.get(
                attribute_name
            )

            if convert_method:
                new_object_type_data[attribute_name] = convert_method(
                    value=value, object_type_instance=object_type_instance
                )

        return new_object_type_data

    def _convert_parameter_type_independent_values(
        self, parameter_type_instance: dict
    ):
        new_parameter_type_instance = dict()
        convert_method_by_attribute_name = {
            "multiple": self._utils.convert_to_bool,
            "required": self._utils.convert_to_bool,
            "returnable": self._utils.convert_to_bool,
            "line_type": self._utils.convert_to_string,
            "name": self._utils.convert_to_string,
            "materialize": self._utils.convert_to_bool,
            "minimize": self._utils.convert_to_bool,
            "inherit_location": self._utils.convert_to_bool,
            "virtual": self._utils.convert_to_bool,
            "global_uniqueness": self._utils.convert_to_bool,
            "val_type": self._utils.convert_to_string,
        }
        for attribute_name, value in parameter_type_instance.items():
            convert_method = convert_method_by_attribute_name.get(
                attribute_name
            )

            if convert_method:
                new_parameter_type_instance[attribute_name] = convert_method(
                    value=value
                )

        new_parameter_type_instance["created_by"] = self._username
        new_parameter_type_instance["modified_by"] = self._username
        return new_parameter_type_instance

    def _convert_parameter_type_dependent_values(
        self, parameter_type_instance: TPRM
    ):
        new_parameter_type_instance = dict()
        convert_method_by_attribute_name = {
            "constraint": self._utils.process_constraint_value,
            "prm_link_filter": self._utils.process_prm_link_filter_value,
            "backward_link": self._utils.process_backward_link_value,
        }

        for attribute_name, value in dict(parameter_type_instance).items():
            convert_method = convert_method_by_attribute_name.get(
                attribute_name
            )

            if convert_method:
                new_parameter_type_instance[attribute_name] = convert_method(
                    value=value,
                    parameter_type_instance=parameter_type_instance,
                )

        return new_parameter_type_instance

    def _check_object_type_exists(self):
        """
        TMO name has to be unique
        So we need to check if object type with names from sheets are already exists
        """
        query = select(TMO.name).where(
            TMO.name.in_(
                self._requested_data.object_type_instances_by_name.keys()
            )
        )
        object_type_exists = self._session.execute(query).scalars().all()
        if object_type_exists:
            raise ObjectTypeAlreadyExists(
                status_code=422,
                detail=f"Object types {object_type_exists} already exists.",
            )

    def _convert_independent_object_type_data(self):
        """
        In TMO there are dependent attributes (latitude, status, etc.) that rely on
        parameter types which do not yet exist. These parameters must be created by
        this object type, creating a circular dependency.

        To break this cycle, we first create the base data so that IDs can be retrieved
        by the requested names from the database.
        """
        for (
            object_type_name,
            object_type_instance,
        ) in self._requested_data.object_type_instances_by_name.items():
            new_object_type_data = self._convert_object_type_independent_values(
                object_type_instance=object_type_instance,
            )
            object_type_instance.update(new_object_type_data)
            new_object_type_data = TMO(**new_object_type_data)
            self._session.add(new_object_type_data)
            self._session.flush()
            object_type_instance["id"] = new_object_type_data.id

    def _convert_dependent_object_type_data(self):
        """
        In TMO there are dependent attributes (latitude, status, etc.) that rely on
        parameter types which do not yet exist. These parameters must be created by
        this object type, creating a circular dependency.

        To break this cycle, we first create the base data so that IDs can be retrieved
        by the requested names from the database.
        """
        for (
            object_type_name,
            object_type_instance,
        ) in self._requested_data.object_type_instances_by_name.items():
            new_object_type_data = self._convert_object_type_dependent_values(
                object_type_instance=TMO(**object_type_instance),
            )
            object_type_instance.update(new_object_type_data)
            existing_instance = self._session.get(
                TMO, object_type_instance["id"]
            )
            for attr_name, attr_value in new_object_type_data.items():
                setattr(existing_instance, attr_name, attr_value)
            self._session.flush()

    def _convert_independent_parameter_type_data(self):
        for (
            object_type_name,
            parameter_type_instances,
        ) in self._requested_data.parameter_type_instances_by_object_type_name.items():
            object_type_instance = (
                self._requested_data.object_type_instances_by_name[
                    object_type_name
                ]
            )

            for parameter_type_instance in parameter_type_instances:
                new_parameter_type_instance = (
                    self._convert_parameter_type_independent_values(
                        parameter_type_instance=parameter_type_instance
                    )
                )
                new_parameter_type_instance["tmo_id"] = object_type_instance[
                    "id"
                ]
                parameter_type_instance.update(new_parameter_type_instance)

                created_instance = TPRM(**new_parameter_type_instance)
                self._session.add(created_instance)
                self._session.flush()
                parameter_type_instance["id"] = created_instance.id
                parameter_type_instance["tmo_id"] = created_instance.tmo_id

    def _convert_dependent_parameter_type_data(self):
        parameter_types_by_object_type_name = (
            self._requested_data.parameter_type_instances_by_object_type_name
        )
        for (
            object_type_name,
            parameter_type_instances,
        ) in parameter_types_by_object_type_name.items():
            for parameter_type_instance in parameter_type_instances:
                new_parameter_type_data = (
                    self._convert_parameter_type_dependent_values(
                        parameter_type_instance=TPRM(**parameter_type_instance),
                    )
                )

                existing_param = self._session.get(
                    TPRM, parameter_type_instance["id"]
                )
                for attr_name, attr_value in new_parameter_type_data.items():
                    setattr(existing_param, attr_name, attr_value)

                self._session.flush()

    def execute(self):
        sheets_as_dataframes = self._get_dataframe_from_file_data()

        self._requested_data = self._get_dataframe_data_in_dict(
            sheets_as_dataframes=sheets_as_dataframes
        )

        self._check_object_type_exists()
        self._convert_independent_object_type_data()
        self._convert_independent_parameter_type_data()
        self._convert_dependent_object_type_data()
        self._convert_dependent_parameter_type_data()
        self._session.commit()
