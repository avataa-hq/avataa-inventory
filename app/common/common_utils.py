import re
import time
from ast import literal_eval
from datetime import datetime, timedelta
from typing import Any, Union

from sqlalchemy import func, cast
from sqlmodel import Session, select, Integer

from common.common_constant import VALID_DATETIME_FORMATS, VALID_DATE_FORMATS
from common.common_exceptions import ValidationError
from database import get_chunked_values_by_sqlalchemy_limit
from models import TPRM, MO, PRM
from routers.parameter_router.schemas import PRMCreateByMO
from services.listener_service.constants import PARAMETER_TYPE_INSTANCES_CACHE
from val_types.constants import (
    enum_val_type_name,
    two_way_mo_link_val_type_name,
)


def unpack_dict_values(dict_with_mixed_values: dict[Any, Union[list]]):
    """
    this method get`s dict, where values can be as list of values, as single value
    and return list of this mixed type values
    """
    data = []
    for _, obj_ids in dict_with_mixed_values.items():
        if isinstance(obj_ids, list):
            data.extend(obj_ids)
            continue
        data.append(obj_ids)
    return data


class ValueTypeValidator:
    def __init__(
        self,
        session: Session,
        parameter_type_instance: TPRM,
        value_to_validate: Any,
    ):
        self._session = session
        self._parameter_type_instance = parameter_type_instance
        self._value_to_validate = value_to_validate

    def validate(self):
        if self._parameter_type_instance.multiple:
            validation_task = MultipleTPRMValidation(
                session=self._session,
                value_to_validate=self._value_to_validate,
                parameter_type_instance=self._parameter_type_instance,
            )

        else:
            validation_task = SingleTPRMValidation(
                session=self._session,
                parameter_type_instance=self._parameter_type_instance,
                value_to_validate=self._value_to_validate,
            )

        validated_value = validation_task.validate()
        return validated_value


class ConstraintProcessor:
    @staticmethod
    def string_constraint_processor(value_to_validate: Any, constraint: str):
        pattern = re.compile(rf"{constraint}")
        if isinstance(value_to_validate, list):
            for value in value_to_validate:
                if not isinstance(value, str) or not pattern.match(value):
                    raise ValidationError(
                        status_code=422,
                        detail=f"Parameter does not valid by constraint {constraint}",
                    )
        else:
            if not isinstance(value_to_validate, str) or not pattern.match(
                value_to_validate
            ):
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameter does not valid by constraint {constraint}",
                )

    @staticmethod
    def integer_constraint_processor(value_to_validate: Any, constraint: str):
        bottom, top = constraint.split(":")
        top = int(top)
        bottom = int(bottom)

        if isinstance(value_to_validate, list):
            if all(bottom < int(value) < top for value in value_to_validate):
                return True

            raise ValidationError(
                status_code=422,
                detail=f"Parameter does not valid by constraint. Values have to be between {bottom} and {top}",
            )
        if bottom < int(value_to_validate) < top:
            return True

        raise ValidationError(
            status_code=422,
            detail=f"Parameter does not valid by constraint. Values have to be between {bottom} and {top}",
        )

    @staticmethod
    def float_constraint_processor(value_to_validate: Any, constraint: str):
        bottom, top = constraint.split(":")
        top = float(top)
        bottom = float(bottom)

        if isinstance(value_to_validate, list):
            if all(bottom < float(value) < top for value in value_to_validate):
                return True

            raise ValidationError(
                status_code=422,
                detail=f"Parameter does not valid by constraint. Values have to be between {bottom} and {top}",
            )
        if bottom < float(value_to_validate) < top:
            return True

        raise ValidationError(
            status_code=422,
            detail=f"Parameter does not valid by constraint. Values have to be between {bottom} and {top}",
        )

    @staticmethod
    def mo_link_constraint_processor(
        value_to_validate: list[int], constraint: int, session: Session
    ):
        if isinstance(value_to_validate, list):
            exists_objects = []

            for chunk in get_chunked_values_by_sqlalchemy_limit(
                value_to_validate
            ):
                stmt = select(MO.id).where(
                    MO.id.in_(chunk), MO.tmo_id == int(constraint)
                )
                exists_objects.extend(
                    mo_name for mo_name in session.execute(stmt).scalars().all()
                )

            not_exists_values = set(value_to_validate).difference(
                set(exists_objects)
            )

            if not_exists_values:
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameter does not valid by constraint. "
                    f"Object not exists in object type with id {constraint}",
                )
            return value_to_validate

        else:
            stmt = select(MO.id).where(
                MO.id == value_to_validate, MO.tmo_id == int(constraint)
            )
            object_exists = session.execute(stmt).scalar()

            if object_exists:
                return value_to_validate

            raise ValidationError(
                status_code=422,
                detail=f"Parameter does not valid by constraint. "
                f"Object not exists in object type with id {constraint}",
            )

    @staticmethod
    def prm_link_constraint_processor(
        value_to_validate: list[int], constraint: int, session: Session
    ):
        if isinstance(value_to_validate, list):
            exists_parameters = []

            for chunk in get_chunked_values_by_sqlalchemy_limit(
                values=value_to_validate
            ):
                stmt = select(PRM.id).where(
                    PRM.id.in_(chunk), PRM.tprm_id == int(constraint)
                )
                exists_parameters.extend(
                    mo_name for mo_name in session.execute(stmt).scalars().all()
                )

            not_exists_values = set(value_to_validate).difference(
                set(exists_parameters)
            )
            if not_exists_values:
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameters are not valid for constraint. "
                    f"Values are not exists for parameter type with id {constraint}",
                )

            return value_to_validate

        else:
            stmt = select(PRM.id).where(
                PRM.id == value_to_validate, PRM.tprm_id == int(constraint)
            )
            parameter_exists = session.execute(stmt).scalar()

            if parameter_exists:
                return value_to_validate

            raise ValidationError(
                status_code=422,
                detail=f"Parameters are not valid for constraint. "
                f"Values are not exists for parameter type with id {constraint}",
            )

    @staticmethod
    def enum_constraint_processor(
        value_to_validate: list[int], constraint: int
    ):
        if isinstance(value_to_validate, list):
            for value in value_to_validate:
                if value in constraint:
                    continue
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameters are not valid for constraint. "
                    f"Requested value not in list of values {constraint}",
                )
            return value_to_validate

        if value_to_validate in constraint:
            return value_to_validate

        raise ValidationError(
            status_code=422,
            detail=f"Parameters are not valid for constraint. "
            f"Requested value not in list of values {constraint}",
        )

    @staticmethod
    def prm_link_filter_validator(
        session: Session,
        parameter_type_instance: TPRM,
        value_to_validate: list[PRM],
    ):
        regex = re.compile(r"(\d+):(\d+)")
        internal_tprm_id, external_tprm_id = regex.findall(
            parameter_type_instance.prm_link_filter
        )[0]
        internal_tprm_id = int(internal_tprm_id)
        external_tprm_id = int(external_tprm_id)

        internal_parameter_link = None
        for parameter in value_to_validate:
            object_instance = session.get(MO, parameter.mo_id)

            for object_parameter in object_instance.prms:
                if object_parameter.tprm_id == internal_tprm_id:
                    internal_parameter_link = object_parameter
                    break

            if internal_parameter_link:
                possible_prm_ids = get_possible_prm_ids_for_internal_link(
                    session=session,
                    external_tprm_id=external_tprm_id,
                    internal_parameter_link=internal_parameter_link,
                    db_param_type=parameter_type_instance,
                )
            else:
                possible_prm_ids = get_possible_prm_ids_for_external_link(
                    session=session,
                    external_tprm_id=external_tprm_id,
                    db_param_type=parameter_type_instance,
                )

            linked_parameter = session.execute(
                select(PRM).where(PRM.id == parameter.value)
            ).scalar()

            if linked_parameter.id not in possible_prm_ids:
                raise ValidationError(
                    status_code=422,
                    detail="Parameters are not valid for parameter link filter. "
                    f"For internal parameter type id: {internal_tprm_id} and "
                    f"external parameter type id: {external_tprm_id}",
                )


class MultipleTPRMValidation(ConstraintProcessor):
    def __init__(
        self,
        session: Session,
        parameter_type_instance: TPRM,
        value_to_validate: Any,
    ):
        self._session = session
        self._parameter_type_instance = parameter_type_instance
        self._value_to_validate = value_to_validate

    def _multiple_string_val_type_validation(self):
        if self._parameter_type_instance.constraint:
            self.string_constraint_processor(
                value_to_validate=self._value_to_validate,
                constraint=self._parameter_type_instance.constraint,
            )
            return self._value_to_validate

        return self._value_to_validate

    def _multiple_integer_val_type_validation(self):
        for value in self._value_to_validate:
            if isinstance(value, bool):
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameter {value} does not integer format",
                )

            try:
                int(value)
            except (ValueError, TypeError):
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameter {value} does not integer format",
                )

        if self._parameter_type_instance.constraint:
            self.integer_constraint_processor(
                value_to_validate=self._value_to_validate,
                constraint=self._parameter_type_instance.constraint,
            )

        return [int(value) for value in self._value_to_validate]

    def _multiple_float_val_type_validation(self):
        for value in self._value_to_validate:
            if isinstance(value, bool):
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameter {value} does not float format",
                )

            try:
                float(value)
            except (ValueError, TypeError):
                raise ValidationError(
                    status_code=422,
                    detail=f"Parameter {value} does not float format",
                )

        if self._parameter_type_instance.constraint:
            self.float_constraint_processor(
                value_to_validate=self._value_to_validate,
                constraint=self._parameter_type_instance.constraint,
            )

        return [float(value) for value in self._value_to_validate]

    def _multiple_date_val_type_validation(self):
        for value in self._value_to_validate:
            matched_format = False

            for date_format in VALID_DATE_FORMATS:
                try:
                    datetime.strptime(value, date_format)
                    matched_format = True
                    break

                except (TypeError, ValueError):
                    continue

            if matched_format:
                continue

            raise ValidationError(
                status_code=422,
                detail=f"Parameter {value} does not date format {VALID_DATE_FORMATS}",
            )

        return self._value_to_validate

    def _multiple_datetime_val_type_validation(self):
        for value in self._value_to_validate:
            matched_format = False

            for datetime_format in VALID_DATETIME_FORMATS:
                try:
                    datetime.strptime(value, datetime_format)
                    matched_format = True
                    break

                except (TypeError, ValueError):
                    continue

            if matched_format:
                continue

            raise ValidationError(
                status_code=422,
                detail=f"Parameter {value} does not datetime format {VALID_DATETIME_FORMATS}",
            )
        return self._value_to_validate

    def _multiple_bool_val_type_validation(self):
        allowed_values = {"true", "false", "0", "1"}

        for value in self._value_to_validate:
            value = str(value).lower()
            if value.lower() in allowed_values:
                continue

            raise ValidationError(
                status_code=422,
                detail=f"Parameter {value} does not in boolean values in any case: {allowed_values}",
            )

        return [bool(value) for value in self._value_to_validate]

    def _multiple_mo_link_val_type_validation(self):
        if self._parameter_type_instance.constraint:
            self.mo_link_constraint_processor(
                session=self._session,
                value_to_validate=self._value_to_validate,
                constraint=int(self._parameter_type_instance.constraint),
            )

        else:
            exist_objects = []

            for chunk in get_chunked_values_by_sqlalchemy_limit(
                self._value_to_validate
            ):
                stmt = select(MO.id).where(MO.id.in_(chunk))
                exist_objects.extend(
                    mo_name
                    for mo_name in self._session.execute(stmt).scalars().all()
                )

            not_exists_values = set(self._value_to_validate).difference(
                set(exist_objects)
            )
            if not_exists_values:
                raise ValidationError(
                    status_code=422,
                    detail="Parameter does not valid.\nRequested object to be linked are not exists",
                )

        return self._value_to_validate

    def _multiple_prm_link_val_type_validation(self):
        if self._parameter_type_instance.prm_link_filter:
            self.prm_link_filter_validator(
                session=self._session,
                parameter_type_instance=self._parameter_type_instance,
                value_to_validate=self._value_to_validate,
            )

        if self._parameter_type_instance.constraint:
            self.prm_link_constraint_processor(
                session=self._session,
                value_to_validate=self._value_to_validate,
                constraint=int(self._parameter_type_instance.constraint),
            )

        else:
            exist_parameters = []
            for chunk in get_chunked_values_by_sqlalchemy_limit(
                self._value_to_validate
            ):
                stmt = select(MO.name).where(PRM.id.in_(chunk))
                exist_parameters.extend(
                    mo_name
                    for mo_name in self._session.execute(stmt).scalars().all()
                )

            not_exists_values = set(self._value_to_validate).difference(
                set(exist_parameters)
            )
            if not_exists_values:
                raise ValidationError(
                    status_code=422,
                    detail="Parameter does not valid.\nRequested parameters to be linked are not exists",
                )
        return self._value_to_validate

    def _multiple_enum_val_type_validation(self):
        return self.enum_constraint_processor(
            value_to_validate=self._value_to_validate,
            constraint=literal_eval(self._parameter_type_instance.constraint),
        )

    def validate(self):
        validation_methods_by_val_type: dict[str, func] = {
            "str": self._multiple_string_val_type_validation,
            "int": self._multiple_integer_val_type_validation,
            "float": self._multiple_float_val_type_validation,
            "date": self._multiple_date_val_type_validation,
            "datetime": self._multiple_datetime_val_type_validation,
            "bool": self._multiple_bool_val_type_validation,
            "mo_link": self._multiple_mo_link_val_type_validation,
            "prm_link": self._multiple_prm_link_val_type_validation,
            enum_val_type_name: self._multiple_enum_val_type_validation,
            two_way_mo_link_val_type_name: self._multiple_mo_link_val_type_validation,
        }
        if (
            self._parameter_type_instance.val_type
            not in validation_methods_by_val_type
        ):
            return []

        if self._value_to_validate:
            if isinstance(self._value_to_validate, list):
                validation_method = validation_methods_by_val_type[
                    self._parameter_type_instance.val_type
                ]
                return validation_method()

        raise ValidationError(
            status_code=422, detail="Parameter does not valid.\nValue is empty"
        )


class SingleTPRMValidation(ConstraintProcessor):
    def __init__(
        self,
        session: Session,
        parameter_type_instance: TPRM,
        value_to_validate: Any,
    ):
        self._session = session
        self._parameter_type_instance = parameter_type_instance
        self._value_to_validate = value_to_validate

    def _string_val_type_validation(self):
        if self._parameter_type_instance.constraint:
            self.string_constraint_processor(
                value_to_validate=self._value_to_validate,
                constraint=self._parameter_type_instance.constraint,
            )

        return self._value_to_validate

    def _sequence_val_type_validation(self):
        if self._value_to_validate is None or isinstance(
            self._value_to_validate, int
        ):
            return self._value_to_validate
        elif self._value_to_validate.isdigit():
            query = select(
                func.max(cast(self._value_to_validate, Integer))
            ).where(PRM.tprm_id == self._parameter_type_instance.id)
            max_value = self._session.execute(query)
            max_value = max_value.scalar() or 0
            if int(self._value_to_validate) <= 0:
                raise ValidationError(
                    status_code=422,
                    detail="Parameter is not valid. Sequence value can not be less than zero",
                )

            if int(self._value_to_validate) > max_value + 1:
                raise ValidationError(
                    status_code=422,
                    detail="Parameter is not valid. Sequence value more than limit",
                )
        else:
            raise ValidationError(
                status_code=422,
                detail="Parameter is not valid. Sequence value have to be integer",
            )

    def _integer_val_type_validation(self):
        if isinstance(self._value_to_validate, bool):
            raise ValidationError(
                status_code=422,
                detail=f"Parameter {self._value_to_validate} does not integer format",
            )

        try:
            int(self._value_to_validate)
        except (ValueError, TypeError):
            raise ValidationError(
                status_code=422,
                detail=f"Parameter {self._value_to_validate} does not integer format",
            )

        if self._parameter_type_instance.constraint:
            self.integer_constraint_processor(
                value_to_validate=self._value_to_validate,
                constraint=self._parameter_type_instance.constraint,
            )

        return int(self._value_to_validate)

    def _float_val_type_validation(self):
        if isinstance(self._value_to_validate, bool):
            raise ValidationError(
                status_code=422,
                detail=f"Parameter {self._value_to_validate} does not float format",
            )

        try:
            float(self._value_to_validate)
        except (ValueError, TypeError):
            raise ValidationError(
                status_code=422,
                detail=f"Parameter {self._value_to_validate} does not float format",
            )

        if self._parameter_type_instance.constraint:
            self.float_constraint_processor(
                value_to_validate=self._value_to_validate,
                constraint=self._parameter_type_instance.constraint,
            )

        return float(self._value_to_validate)

    def _date_val_type_validation(self):
        matched_format = False

        for datetime_format in VALID_DATE_FORMATS:
            try:
                datetime.strptime(self._value_to_validate, datetime_format)
                matched_format = True
                break
            except (TypeError, ValueError):
                continue

        if matched_format:
            return self._value_to_validate

        raise ValidationError(
            status_code=422,
            detail=f"Parameter {self._value_to_validate} does not date format for {VALID_DATE_FORMATS}",
        )

    def _datetime_val_type_validation(self):
        matched_format = False

        for datetime_format in VALID_DATETIME_FORMATS:
            try:
                datetime.strptime(self._value_to_validate, datetime_format)
                matched_format = True
                break
            except (TypeError, ValueError):
                continue

        if matched_format:
            return self._value_to_validate

        raise ValidationError(
            status_code=422,
            detail=f"Parameter {self._value_to_validate} does not datetime format for {VALID_DATETIME_FORMATS}",
        )

    def _bool_val_type_validation(self):
        allowed_values = {"true", "false", "0", "1"}

        value = str(self._value_to_validate).lower()
        if value.lower() in allowed_values:
            self._value_to_validate = bool(self._value_to_validate)
            return self._value_to_validate

        raise ValidationError(
            status_code=422,
            detail=f"Parameter {self._value_to_validate} does not boolean format for {allowed_values}",
        )

    def _mo_link_val_type_validation(self):
        if self._parameter_type_instance.constraint:
            self.mo_link_constraint_processor(
                session=self._session,
                value_to_validate=self._value_to_validate,
                constraint=int(self._parameter_type_instance.constraint),
            )

        else:
            stmt = select(MO.id).where(MO.id == self._value_to_validate)
            object_exists = self._session.execute(stmt).scalar()

            if object_exists:
                return self._value_to_validate

            raise ValidationError(
                status_code=422,
                detail=f"Parameter {self._value_to_validate} does not valid. "
                f"Requested object with id {self._value_to_validate} does not exists",
            )

    def _prm_link_val_type_validation(self):
        if self._parameter_type_instance.prm_link_filter:
            self.prm_link_filter_validator(
                session=self._session,
                parameter_type_instance=self._parameter_type_instance,
                value_to_validate=[self._value_to_validate],
            )

        if self._parameter_type_instance.constraint:
            self.prm_link_constraint_processor(
                session=self._session,
                value_to_validate=self._value_to_validate,
                constraint=int(self._parameter_type_instance.constraint),
            )

        else:
            stmt = select(PRM.id).where(PRM.id == self._value_to_validate)
            parameter_exists = self._session.execute(stmt).scalar()

            if parameter_exists:
                return self._value_to_validate

            raise ValidationError(
                status_code=422,
                detail=f"Parameter {self._value_to_validate} does not valid. "
                f"Requested parameter with value {self._value_to_validate} does not exists",
            )

    def _enum_val_type_validation(self):
        self.enum_constraint_processor(
            value_to_validate=self._value_to_validate,
            constraint=literal_eval(self._parameter_type_instance.constraint),
        )

    def validate(self):
        validation_methods_by_val_type: dict[str, func] = {
            "str": self._string_val_type_validation,
            "int": self._integer_val_type_validation,
            "float": self._float_val_type_validation,
            "date": self._date_val_type_validation,
            "datetime": self._datetime_val_type_validation,
            "bool": self._bool_val_type_validation,
            "mo_link": self._mo_link_val_type_validation,
            "prm_link": self._prm_link_val_type_validation,
            "sequence": self._sequence_val_type_validation,
            enum_val_type_name: self._enum_val_type_validation,
            two_way_mo_link_val_type_name: self._mo_link_val_type_validation,
        }
        if (
            self._parameter_type_instance.val_type
            not in validation_methods_by_val_type
        ):
            return []

        validation_method = validation_methods_by_val_type[
            self._parameter_type_instance.val_type
        ]

        return validation_method()


def get_possible_prm_ids_for_internal_link(
    session: Session,
    external_tprm_id: int,
    internal_parameter_link: PRM | PRMCreateByMO,
    db_param_type: TPRM,
) -> list[int]:
    linked_param_value = session.get(
        PRM, int(internal_parameter_link.value)
    ).value

    stmt = select(PRM.mo_id).where(
        PRM.tprm_id == int(external_tprm_id), PRM.value == linked_param_value
    )
    possible_mo_ids = session.execute(stmt).scalars().all()

    stmt = select(PRM.id).where(
        PRM.tprm_id == int(db_param_type.constraint),
        PRM.mo_id.in_(possible_mo_ids),
    )
    possible_prm_ids = session.execute(stmt).scalars().all()
    return possible_prm_ids


def get_possible_prm_ids_for_external_link(
    session: Session, external_tprm_id: int, db_param_type: TPRM
):
    linked_mo_ids = session.exec(
        select(PRM.mo_id).where(PRM.tprm_id == int(db_param_type.constraint))
    ).all()
    external_mo_ids = session.exec(
        select(PRM.mo_id).where(PRM.tprm_id == int(external_tprm_id))
    ).all()

    possible_mo_ids = set(linked_mo_ids).difference(set(external_mo_ids))

    stmt = select(PRM.id).where(
        PRM.tprm_id == int(db_param_type.constraint),
        PRM.mo_id.in_(possible_mo_ids),
    )

    possible_prm_ids = session.execute(stmt).scalars().all()
    return possible_prm_ids


def clear_event_cache_daily():
    while True:
        now = datetime.utcnow()
        next_clear = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_seconds = (next_clear - now).total_seconds()

        time.sleep(sleep_seconds)
        PARAMETER_TYPE_INSTANCES_CACHE.clear()
