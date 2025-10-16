import ast
import copy
import pickle
from dataclasses import dataclass
from typing import List, Dict

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TPRM, PRM
from routers.parameter_router.schemas import PRMCreateByMO
from val_types.constants import enum_val_type_name
from val_types.enum_val_type.exceptions import (
    ParameterValueNotValidWithConstraint,
    ParameterAlreadyExists,
)
from val_types.constants import ErrorHandlingType


@dataclass
class CreateEnumPRMs:
    errors: List[str]
    created_parameters: List[PRM]


class EnumPRMCreator:
    def __init__(
        self,
        session: Session,
        tprm_instances: List[TPRM],
        parameters_by_object_id: Dict[int, List[PRMCreateByMO]],
        autocommit: bool = True,
        in_case_of_error: ErrorHandlingType = ErrorHandlingType.RAISE_ERROR,
    ):
        self.session = session
        self.tprm_instances = {
            tprm.id: tprm
            for tprm in tprm_instances
            if tprm.val_type == enum_val_type_name
        }
        self.parameters_by_object_id = parameters_by_object_id
        self.autocommit = autocommit
        self.in_case_of_error = in_case_of_error
        self.errors = []
        self.created_prms = []

    def handle_error(self, error_instance):
        if self.in_case_of_error == ErrorHandlingType.RAISE_ERROR:
            raise error_instance
        self.errors.append(error_instance.detail)

    def validate_enum_parameters(self):
        for object_id, params in self.parameters_by_object_id.items():
            valid_params = []
            for param in params:
                tprm_instance = self.tprm_instances.get(param.tprm_id)
                if not tprm_instance:
                    continue

                tprm_constraint = ast.literal_eval(tprm_instance.constraint)
                if tprm_instance.multiple:
                    invalid_values = [
                        v for v in param.value if v not in tprm_constraint
                    ]
                    if invalid_values:
                        self.handle_error(
                            ParameterValueNotValidWithConstraint(
                                status_code=422,
                                detail=f"Parameter {invalid_values} is not valid for"
                                f" TPRM {tprm_instance.name} constraint",
                            )
                        )
                        continue
                elif param.value not in tprm_constraint:
                    self.handle_error(
                        ParameterValueNotValidWithConstraint(
                            status_code=422,
                            detail=f"Parameter {param.value} is not valid for TPRM {tprm_instance.name} constraint",
                        )
                    )
                    continue
                valid_params.append(param)
            self.parameters_by_object_id[object_id] = valid_params

    def check_parameters_exists(self):
        for object_id, params in self.parameters_by_object_id.items():
            for param in params:
                tprm_instance = self.tprm_instances[param.tprm_id]
                parameter_exists = self.session.execute(
                    select(PRM).where(
                        PRM.tprm_id == tprm_instance.id, PRM.mo_id == object_id
                    )
                ).scalar()
                if parameter_exists:
                    self.handle_error(
                        ParameterAlreadyExists(
                            status_code=422,
                            detail=f"Parameter for {tprm_instance.name} and object with id {object_id} already exists",
                        )
                    )
                    params.remove(param)

    def create_enum_parameters(self) -> CreateEnumPRMs:
        self.validate_enum_parameters()
        self.check_parameters_exists()

        new_prms = []
        for object_id, params in self.parameters_by_object_id.items():
            for param in params:
                tprm_instance = self.tprm_instances[param.tprm_id]
                value = (
                    pickle.dumps(param.value).hex()
                    if tprm_instance.multiple
                    else param.value
                )
                new_prm = PRM(
                    version=1,
                    mo_id=object_id,
                    tprm_id=tprm_instance.id,
                    value=value,
                )
                self.session.add(new_prm)
                self.session.flush()
                new_prms.append(new_prm)

        self.session.flush()
        self.created_prms = copy.deepcopy(new_prms)

        if self.autocommit:
            self.session.commit()

        return CreateEnumPRMs(
            errors=self.errors, created_parameters=self.created_prms
        )
