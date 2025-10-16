import ast
import pickle
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TPRM, TMO, MO, PRM
from routers.parameter_type_router.schemas import TPRMUpdate
from val_types.enum_val_type.exceptions import (
    NotExistsConstraint,
    NotValidConstraint,
    FieldValueMissing,
    FieldValueNotValidWithConstraint,
    DuplicatedTPRMNameInRequest,
    NotFoundParameterType,
    VersionIsNotActual,
    ForceIsNotActivated,
    TPRMWithThisNameAlreadyExists,
)
from val_types.constants import ErrorHandlingType


@dataclass
class UpdatedEnumTPRMs:
    errors: List[str]
    updated_param_types: List[TPRM]


class EnumTPRMUpdater:
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
        self.updated_tprms = []
        self.errors = []

    def handle_error(self, error_instance):
        if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
            raise error_instance
        self.errors.append(error_instance.detail)

    def validate_duplicates(
        self, param_types_for_update: Dict[int, TPRMUpdate]
    ) -> Dict[int, TPRMUpdate]:
        requested_tprm_names = [
            param_type.name for param_type in param_types_for_update.values()
        ]
        duplicates = [
            name
            for name in requested_tprm_names
            if requested_tprm_names.count(name) > 1
        ]

        if duplicates:
            self.handle_error(
                DuplicatedTPRMNameInRequest(
                    status_code=422,
                    detail=f"There are TPRM names in request, which are duplicated {duplicates}",
                )
            )

            return {
                tprm_id: param_type
                for tprm_id, param_type in param_types_for_update.items()
                if param_type.name not in duplicates
            }

        return param_types_for_update

    def fetch_existing_tprms(
        self, param_type_ids: List[int]
    ) -> Dict[int, TPRM]:
        stmt = select(TPRM).where(
            TPRM.id.in_(param_type_ids),
            TPRM.tmo_id == self.object_type_instance.id,
        )
        existing_tprms = self.session.execute(stmt).scalars().all()
        return {tprm.id: tprm for tprm in existing_tprms}

    def process_param_type(
        self,
        db_param_type: TPRM,
        requested_param_type: TPRMUpdate,
        param_type_constraint: List[str],
    ):
        if db_param_type.required or requested_param_type.required:
            if not requested_param_type.field_value:
                self.handle_error(
                    FieldValueMissing(
                        status_code=422,
                        detail=f"TPRM {requested_param_type.name} is required but missing a field value",
                    )
                )
                return

            field_value = (
                ast.literal_eval(requested_param_type.field_value)
                if db_param_type.multiple
                else [requested_param_type.field_value]
            )

            if all(value in param_type_constraint for value in field_value):
                requested_param_type.field_value = (
                    pickle.dumps(field_value).hex()
                    if db_param_type.multiple
                    else field_value[0]
                )
            else:
                self.handle_error(
                    FieldValueNotValidWithConstraint(
                        status_code=422,
                        detail=f"Field value for TPRM {requested_param_type.name} doesn't match the constraint",
                    )
                )
                return

        if requested_param_type.constraint != db_param_type.constraint:
            if not requested_param_type.force:
                self.handle_error(
                    ForceIsNotActivated(
                        status_code=422,
                        detail="To update constraint you have to activate force attribute. "
                        "Data will be changed, if parameters will not match updated constraint",
                    )
                )
                return

            self.update_parameters_for_new_constraint(
                db_param_type=db_param_type,
                new_constraint=requested_param_type.constraint,
            )

            if requested_param_type.required:
                self.create_prms_for_all_mo(
                    db_param_type=db_param_type,
                    field_value=requested_param_type.field_value,
                )
            db_param_type.constraint = requested_param_type.constraint

        self.update_db_param_type_fields(
            db_param_type=db_param_type,
            requested_param_type=requested_param_type,
        )
        self.session.add(db_param_type)
        self.session.flush()

    def update_db_param_type_fields(
        self, db_param_type: TPRM, requested_param_type: TPRMUpdate
    ):
        updated_param_type = requested_param_type.dict(exclude_unset=True)
        updated_param_type.pop("force", None)

        for key, value in updated_param_type.items():
            setattr(db_param_type, key, value)

        db_param_type.version += 1
        db_param_type.modification_date = datetime.utcnow()

    def update_parameters_for_new_constraint(
        self, db_param_type: TPRM, new_constraint
    ):
        new_constraint = ast.literal_eval(new_constraint)

        stmt = select(PRM).where(PRM.tprm_id == db_param_type.id)
        exists_parameters = self.session.execute(stmt).scalars().all()

        for parameter in exists_parameters:
            if db_param_type.multiple:
                parameter_value = pickle.loads(bytes.fromhex(parameter.value))
                new_parameter_value = [
                    value
                    for value in parameter_value
                    if value in new_constraint
                ]
                if new_parameter_value:
                    parameter.value = pickle.dumps(parameter_value).hex()
                    self.session.add(parameter)

            else:
                if parameter.value not in map(str, new_constraint):
                    self.session.delete(parameter)

    def create_prms_for_all_mo(self, db_param_type: TPRM, field_value: Any):
        if db_param_type.multiple:
            field_value = ast.literal_eval(field_value)

        stmt = select(MO.id).where(MO.tmo_id == self.object_type_instance.id)
        all_mo_ids = self.session.execute(stmt).scalars().all()

        stmt = select(PRM.mo_id).where(
            PRM.mo_id.in_(all_mo_ids), PRM.tprm_id == db_param_type.id
        )
        existing_mo_ids = self.session.execute(stmt).scalars().all()

        missing_mo_ids = set(all_mo_ids) - set(existing_mo_ids)

        for mo_id in missing_mo_ids:
            new_prm = PRM(
                version=1,
                mo_id=mo_id,
                value=field_value,
                tprm_id=db_param_type.id,
            )
            self.session.add(new_prm)
            self.session.flush()

    def check_duplicated_names(
        self, param_types_for_update: Dict[int, TPRMUpdate]
    ):
        new_param_types_for_update = {}
        for tprm_id, param_type in param_types_for_update.items():
            stmt = select(TPRM.name).where(
                TPRM.name == param_type.name,
                TPRM.id != tprm_id,
                TPRM.tmo_id == self.object_type_instance.id,
            )
            duplicated_tprm_name = self.session.execute(stmt).scalar()

            if duplicated_tprm_name:
                self.handle_error(
                    TPRMWithThisNameAlreadyExists(
                        status_code=422,
                        detail=f"There is TPRM name in request, which already exist {duplicated_tprm_name}",
                    )
                )

            else:
                new_param_types_for_update[tprm_id] = param_type

        return new_param_types_for_update

    def update_param_types(
        self, param_types_for_update: Dict[int, TPRMUpdate]
    ) -> UpdatedEnumTPRMs:
        param_types_for_update = self.validate_duplicates(
            param_types_for_update
        )
        param_types_for_update = self.check_duplicated_names(
            param_types_for_update
        )

        tprm_instance_by_id = self.fetch_existing_tprms(
            list(param_types_for_update.keys())
        )
        for tprm_id, param_type in param_types_for_update.items():
            db_param_type = tprm_instance_by_id.get(tprm_id)

            if not db_param_type:
                self.handle_error(
                    NotFoundParameterType(
                        status_code=422,
                        detail=f"TPRM with name {param_type.name} does not exist in "
                        f"TMO {self.object_type_instance.name}",
                    )
                )
                continue

            if db_param_type.version != param_type.version:
                self.handle_error(
                    VersionIsNotActual(
                        status_code=422,
                        detail=f"Actual version for TPRM with name {param_type.name} is {db_param_type.version}",
                    )
                )
                continue

            if not param_type.constraint:
                self.handle_error(
                    NotExistsConstraint(
                        status_code=422,
                        detail=f"Enum val type can't be created without constraint. "
                        f"TPRM {param_type.name} doesn't have it",
                    )
                )
                continue

            try:
                param_type_constraint = ast.literal_eval(param_type.constraint)
            except ValueError:
                self.handle_error(
                    NotValidConstraint(
                        status_code=422,
                        detail=f"Enum constraint must be a list of values."
                        f" Invalid constraint for TPRM {param_type.name}",
                    )
                )
                continue

            self.process_param_type(
                db_param_type, param_type, param_type_constraint
            )
            self.updated_tprms.append(db_param_type)

        if self.autocommit:
            self.session.commit()

        return UpdatedEnumTPRMs(
            errors=self.errors, updated_param_types=self.updated_tprms
        )
