import ast
import copy
import pickle
from dataclasses import dataclass
from typing import List, Dict

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TPRM, PRM
from routers.parameter_router.schemas import PRMUpdateByMO
from val_types.constants import enum_val_type_name
from val_types.enum_val_type.exceptions import (
    ParameterValueNotValidWithConstraint,
    VersionIsNotActual,
    ParameterNotExists,
)
from val_types.constants import ErrorHandlingType


@dataclass
class UpdateEnumPRMs:
    errors: List[str]
    updated_parameters: List[PRM]


class EnumPRMUpdator:
    def __init__(
        self,
        session: Session,
        tprm_instances: List[TPRM],
        parameters_by_object_id: Dict[int, List[PRMUpdateByMO]],
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
        self.updated_prms = []

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

    def update_enum_parameters(self):
        self.validate_enum_parameters()

        updated_prms = []
        for object_id, params in self.parameters_by_object_id.items():
            for param in params:
                tprm_instance = self.tprm_instances[param.tprm_id]

                parameter_exists = self.session.execute(
                    select(PRM).where(
                        PRM.tprm_id == tprm_instance.id, PRM.mo_id == object_id
                    )
                ).scalar()
                if parameter_exists:
                    if parameter_exists.version != param.version:
                        self.handle_error(
                            VersionIsNotActual(
                                status_code=422,
                                detail=f"Version for parameter with TPRM {tprm_instance.name} "
                                f"and object id {object_id} is not actual",
                            )
                        )

                    value = (
                        pickle.dumps(param.value).hex()
                        if tprm_instance.multiple
                        else param.value
                    )
                    parameter_exists.value = str(value)
                    parameter_exists.version += 1
                    self.session.add(parameter_exists)
                    self.session.flush()
                    updated_prms.append(parameter_exists)

                else:
                    self.handle_error(
                        ParameterNotExists(
                            status_code=422,
                            detail=f"Parameter for {tprm_instance.name} "
                            f"and object id {object_id} is not exists",
                        )
                    )

        self.session.flush()
        self.updated_prms = copy.deepcopy(updated_prms)

        if self.autocommit:
            self.session.commit()

        return UpdateEnumPRMs(
            errors=self.errors, updated_parameters=self.updated_prms
        )
