import pickle

from fastapi import BackgroundTasks
from sqlalchemy.orm import Session

from common.common_constant import val_types_with_required_constraint
from functions.db_functions.db_add import (
    add_required_params_for_objects_when_create_param_type,
)
from functions.functions_dicts import param_type_constraint_validation
from functions.functions_utils.utils import session_commit_create_or_exception
from functions.validation_functions.validation_function import (
    val_type_validation_when_create_param_type,
)
from models import TPRM
from routers.object_type_router.utils import ObjectTypeDBGetter
from routers.parameter_router.utils import validate_param_type_if_required
from routers.parameter_type_router.exceptions import (
    NotValidParameterTypePreferences,
    ThisValueTypeNeedConstraint,
)
from routers.parameter_type_router.schemas import TPRMCreate
from routers.parameter_type_router.utils import build_sequence
from services.security_service.utils.get_user_data import (
    get_username_from_session,
)
from val_types.constants import (
    two_way_mo_link_val_type_name,
    ErrorHandlingType,
    enum_val_type_name,
)
from val_types.enum_val_type.tprm.create import EnumTPRMCreator
from val_types.two_way_mo_link_val_type.tprm.create import (
    create_two_way_mo_link_tprms,
)


class CreateParameterType(ObjectTypeDBGetter):
    def __init__(
        self,
        request: TPRMCreate,
        session: Session,
        background_tasks: BackgroundTasks,
    ):
        super().__init__(session=session)
        self._request = request
        self._session = session
        self._background_tasks = background_tasks

    def execute(self):
        object_type_instance = self._get_object_type_instance_by_id(
            object_type_id=self._request.tmo_id
        )
        parameter_type_need_constraint = (
            self._request.val_type.lower() in val_types_with_required_constraint
            and self._request.constraint is None
        )

        if parameter_type_need_constraint:
            raise ThisValueTypeNeedConstraint(
                status_code=422,
                detail=f"Please, pass the constraint parameter. "
                f"It is required for {self._request.val_type} val_type.",
            )

        if self._request.val_type.lower() == two_way_mo_link_val_type_name:
            try:
                _, created_tprms = create_two_way_mo_link_tprms(
                    session=self._session,
                    new_tprms=[self._request],
                    in_case_of_error=ErrorHandlingType.RAISE_ERROR,
                )
                return created_tprms[0]

            except Exception as e:
                raise NotValidParameterTypePreferences(
                    status_code=422, detail=str(e)
                )

        elif self._request.val_type.lower() == enum_val_type_name:
            task = EnumTPRMCreator(
                session=self._session, object_type_instance=object_type_instance
            )
            created_tprms_and_errors = task.create_enum_tprms(
                new_param_types=[self._request]
            )
            return created_tprms_and_errors.created_param_types[0]

        else:
            val_type_validation_when_create_param_type(param_type=self._request)

            if self._request.constraint:
                param_type_constraint_validation[self._request.val_type](
                    self._request, self._session
                )

            if self._request.required:
                if self._request.val_type == "formula":
                    field_value = ""
                else:
                    field_value = validate_param_type_if_required(
                        session=self._session, param_type=self._request
                    )

                    if self._request.multiple:
                        field_value = pickle.dumps(field_value).hex()

            param_type = self._request.dict()

            param_type["created_by"] = get_username_from_session(
                session=self._session
            )
            param_type["modified_by"] = get_username_from_session(
                session=self._session
            )

            if param_type["val_type"] != "prm_link":
                param_type["prm_link_filter"] = None

            db_param_type = TPRM(**param_type)
            self._session.add(db_param_type)
            session_commit_create_or_exception(
                session=self._session,
                message="This parameter type already exists.",
            )
            self._session.refresh(db_param_type)
            self._session.commit()

            if db_param_type.required:
                add_required_params_for_objects_when_create_param_type(
                    session=self._session,
                    db_param_type=db_param_type,
                    field_value=field_value,  # noqa
                )
                self._session.commit()
                self._session.refresh(db_param_type)

            if db_param_type.val_type == "sequence":
                self._background_tasks.add_task(
                    build_sequence, self._session, db_param_type
                )

            return db_param_type
