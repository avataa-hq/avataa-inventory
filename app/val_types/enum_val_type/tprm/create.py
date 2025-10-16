import ast
import pickle
from dataclasses import dataclass
from typing import List, Union

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TPRM, TMO, MO, PRM
from routers.parameter_type_router.schemas import TPRMCreate
from val_types.enum_val_type.exceptions import (
    NotExistsConstraint,
    NotValidConstraint,
    FieldValueMissing,
    FieldValueNotValidWithConstraint,
    DuplicatedTPRMNameInRequest,
    TPRMWithThisNameAlreadyExists,
)
from val_types.constants import ErrorHandlingType


@dataclass
class CreatedEnumTPRMs:
    errors: List[str]
    created_param_types: List[TPRM]


class EnumTPRMCreator:
    def __init__(
        self,
        session: Session,
        object_type_instance: TMO,
        autocommit: bool = True,
        in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
    ):
        self.session = session
        self.object_type_instance = object_type_instance
        self.autocommit = autocommit
        self.in_case_of_error = in_case_of_error
        self.requested_tprm_names = set()
        self.created_tprms = []
        self.errors = []
        self.exist_tprm_names = {
            exist_tprm.name for exist_tprm in object_type_instance.tprms
        }

    def _check_for_duplicate(self, param_type: TPRMCreate):
        if param_type.name in self.requested_tprm_names:
            error = f"TPRM with name {param_type.name} is duplicated in request"
            if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                raise DuplicatedTPRMNameInRequest(status_code=422, detail=error)
            self.errors.append(error)
            return False
        return True

    def _check_for_existing_names(self, requested_tprm_names: set):
        already_exists_tprms = self.exist_tprm_names.intersection(
            requested_tprm_names
        )
        if already_exists_tprms:
            error = f"TPRM names already exist in TMO {self.object_type_instance.name}: {already_exists_tprms}"
            if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                raise TPRMWithThisNameAlreadyExists(
                    status_code=422, detail=error
                )
            self.errors.append(error)

    def _validate_param_type_constraint(self, param_type: TPRMCreate) -> bool:
        if not param_type.constraint:
            error = (
                f"Enum val type can't be created without constraint. TPRM with name "
                f"{param_type.name} doesn't have it"
            )
            if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                raise NotExistsConstraint(status_code=422, detail=error)
            self.errors.append(error)
            return False

        try:
            param_type_constraint = ast.literal_eval(param_type.constraint)
            if (
                not isinstance(param_type_constraint, list)
                or len(param_type_constraint) <= 0
            ):
                raise ValueError
        except ValueError:
            error = f"Enum constraint must be a list of values. Constraint for TPRM {param_type.name} is invalid."
            if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                raise NotValidConstraint(status_code=422, detail=error)
            self.errors.append(error)
            return False

        return True

    def _process_required_field(self, param_type: TPRMCreate):
        if param_type.required:
            if not param_type.field_value:
                error = f"TPRM with name {param_type.name} is required but lacks a field value."
                if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                    raise FieldValueMissing(status_code=422, detail=error)
                self.errors.append(error)
                return False

            try:
                if param_type.multiple:
                    field_value = ast.literal_eval(param_type.field_value)
                    if (
                        not isinstance(field_value, list)
                        or len(field_value) <= 0
                    ):
                        raise ValueError
                    param_type.field_value = field_value

                else:
                    field_value = [param_type.field_value]

                if all(
                    value in ast.literal_eval(param_type.constraint)
                    for value in field_value
                ):
                    return True

                error = f"Field value for TPRM {param_type.name} doesn't match its constraint."
                if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                    raise FieldValueNotValidWithConstraint(
                        status_code=422, detail=error
                    )
                self.errors.append(error)
                return False

            except ValueError:
                error = f"Field value for TPRM {param_type.name} is invalid."
                if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
                    raise FieldValueNotValidWithConstraint(
                        status_code=422, detail=error
                    )
                self.errors.append(error)
                return False

        return True

    def _get_preview_tprm_instance(self, param_type: TPRMCreate):
        db_param_type = TPRM(**param_type.dict(), created_by="", modified_by="")
        db_param_type.tmo_id = self.object_type_instance.id
        self.session.add(db_param_type)
        self.session.flush()
        return db_param_type

    def _process_param_type(self, param_type: TPRMCreate):
        if not self._validate_param_type_constraint(param_type):
            return

        if self._process_required_field(param_type):
            db_param_type = self._get_preview_tprm_instance(param_type)
            self.created_tprms.append(db_param_type)

            if param_type.required:
                if param_type.multiple:
                    field_value = pickle.dumps(param_type.field_value).hex()
                else:
                    field_value = param_type.field_value
                self._create_prms_for_all_mo(
                    db_param_type=db_param_type, field_value=field_value
                )

    def _create_prms_for_all_mo(
        self, db_param_type: TPRM, field_value: Union[str, list]
    ):
        stmt = select(MO.id).where(MO.tmo_id == self.object_type_instance.id)
        all_mo_ids_for_current_tmo = self.session.execute(stmt).scalars().all()

        for object_id in all_mo_ids_for_current_tmo:
            new_prm = PRM(
                version=1,
                mo_id=object_id,
                value=field_value,
                tprm_id=db_param_type.id,
            )
            self.session.add(new_prm)
            self.session.flush()

    def create_enum_tprms(
        self, new_param_types: List[TPRMCreate]
    ) -> CreatedEnumTPRMs:
        valid_param_types = []

        for param_type in new_param_types:
            if self._check_for_duplicate(param_type):
                self.requested_tprm_names.add(param_type.name)
                valid_param_types.append(param_type)

        self._check_for_existing_names(self.requested_tprm_names)

        for param_type in valid_param_types:
            self._process_param_type(param_type)

        if self.autocommit:
            self.session.commit()

        return CreatedEnumTPRMs(
            errors=self.errors, created_param_types=self.created_tprms
        )
