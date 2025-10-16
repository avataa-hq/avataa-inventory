from collections import defaultdict
from typing import List, Tuple, TypeAlias, Union

from fastapi import HTTPException
from sqlmodel import Session, select

from database import get_chunked_values_by_sqlalchemy_limit
from functions.db_functions import db_read, db_create
from models import (
    PRM,
    TMO,
    MO,
    TPRM,
)
from routers.object_router.utils import decode_pickle_data
from routers.parameter_router.schemas import (
    PRMCreateByMO,
    CreateObjectParametersResponse,
    ParameterData,
)
from val_types.constants import (
    two_way_mo_link_val_type_name,
    ErrorHandlingType,
    enum_val_type_name,
)
from val_types.enum_val_type.exceptions import EnumValTypeCustomExceptions
from val_types.enum_val_type.prm.create import EnumPRMCreator
from val_types.two_way_mo_link_val_type.prm.create import (
    create_two_way_mo_link_prms,
)

IdType: TypeAlias = int


class ParameterRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_tprms_and_tmos_by_tprm_ids(
        self, tprm_ids: List[IdType]
    ) -> List[TPRM]:
        result = list()

        for chunk in get_chunked_values_by_sqlalchemy_limit(tprm_ids):
            statement = (
                select(TPRM)
                .join(TMO, TPRM.tmo_id == TMO.id)
                .where(TPRM.id.in_(chunk))
            )
            tprms_with_tmo = self.session.execute(statement).scalars().all()
            result.extend(tprms_with_tmo)
        return result

    def get_tprms_by_ids(self, tprms_ids: List[IdType]) -> List[TPRM]:
        result = list()
        for chunk in get_chunked_values_by_sqlalchemy_limit(tprms_ids):
            result.extend(
                self.session.execute(select(TPRM).where(TPRM.id.in_(chunk)))
                .scalars()
                .all()
            )
        return result

    def get_tmos_by_ids(self, tmo_ids: List[IdType]) -> List[TMO]:
        result = list()
        for chunk in get_chunked_values_by_sqlalchemy_limit(tmo_ids):
            result.extend(
                self.session.execute(select(TMO).where(TMO.id.in_(chunk)))
                .scalars()
                .all()
            )
        return result

    def get_mos_by_ids(self, object_ids: List[IdType]) -> List[MO]:
        result = list()
        for chunk in get_chunked_values_by_sqlalchemy_limit(object_ids):
            result.extend(
                self.session.execute(select(MO).where(MO.id.in_(chunk)))
                .scalars()
                .all()
            )
        return result

    def get_params_by_mo_and_tprm_ids(
        self, mo_ids: List[IdType], tprm_ids: List[IdType]
    ) -> List[Tuple[IdType, IdType]]:
        result = list()

        for mo_chunk in get_chunked_values_by_sqlalchemy_limit(mo_ids):
            for tprm_chunk in get_chunked_values_by_sqlalchemy_limit(tprm_ids):
                stmt = select(PRM.mo_id, PRM.tprm_id).where(
                    PRM.mo_id.in_(mo_chunk), PRM.tprm_id.in_(tprm_chunk)
                )
                result.extend(self.session.exec(stmt).all())
        return result

    def add_prm_to_session(self, prm: PRM) -> None:
        self.session.add(prm)
        self.session.flush()

    def add_object_to_session(self, object_instance: MO) -> None:
        self.session.add(object_instance)


class GetParameters:
    def __init__(
        self,
        session: Session,
        object_id: int,
        parameter_type_id: Union[List[int], None],
    ):
        self._session = session
        self._object_id = object_id
        self._parameter_type_id = parameter_type_id

    def _check_object_exists(self):
        db_read.get_db_object_or_exception(
            session=self._session, object_id=self._object_id
        )

    def check(self):
        self._check_object_exists()

    def execute(self):
        object_instance = self._session.get(MO, self._object_id)

        if self._parameter_type_id is None:
            object_parameters = object_instance.prms
        else:
            object_parameters = (
                self._session.execute(
                    select(PRM).where(
                        PRM.mo_id == self._object_id,
                        PRM.tprm_id.in_(self._parameter_type_id),
                    )
                )
                .scalars()
                .all()
            )

        params_list = db_read.get_params_to_read_with_link_values(
            session=self._session, db_params=object_parameters
        )

        return params_list


class CreateObjectParameters:
    def __init__(
        self, session: Session, object_id: int, params: List[PRMCreateByMO]
    ):
        self._session = session
        self._object_id = object_id
        self._object_parameters = params

        self._result_param_list = []
        self._result_error_list = []
        self._common_val_types = []
        self._parameters_by_parameter_type_id = defaultdict(list)

    def _collect_instances(self):
        self._object_instance = db_read.get_db_object_or_exception(
            session=self._session, object_id=self._object_id
        )

        for parameter in self._object_parameters:
            self._parameters_by_parameter_type_id[parameter.tprm_id].append(
                parameter
            )

        parameter_type_ids = set([i.tprm_id for i in self._object_parameters])
        stmt = select(TPRM).where(
            TPRM.id.in_(parameter_type_ids),
            TPRM.tmo_id == self._object_instance.tmo_id,
        )
        current_object_type_parameter_types = self._session.execute(
            stmt
        ).scalars()

        self._parameter_type_by_val_type = defaultdict(list)
        self._parameter_by_val_type = defaultdict(list)

        for parameter_type_instance in current_object_type_parameter_types:
            self._parameter_type_by_val_type[
                parameter_type_instance.val_type
            ].append(parameter_type_instance)

            parameters_by_parameter_type_id = (
                self._parameters_by_parameter_type_id[
                    parameter_type_instance.id
                ]
            )
            self._parameter_by_val_type[
                parameter_type_instance.val_type
            ].extend(parameters_by_parameter_type_id)

    def _process_two_way_mo_link_parameters(
        self,
        val_type: str,
        parameters: list[PRMCreateByMO],
        parameter_types: list[TPRM],
    ):
        if val_type == two_way_mo_link_val_type_name:
            new_prms = {self._object_id: parameters}
            errors, created_prms = create_two_way_mo_link_prms(
                session=self._session,
                new_parameter_types=new_prms,
                in_case_of_error=ErrorHandlingType.PROCESS_CLEARED,
                autocommit=False,
                parameter_types=parameter_types,
            )
            self._result_param_list.extend(created_prms)
            self._result_error_list.extend(errors)

    def _process_enum_parameters(
        self,
        val_type: str,
        parameters: list[PRMCreateByMO],
        parameter_types: list[TPRM],
    ):
        if val_type == enum_val_type_name:
            new_prms = {self._object_id: parameters}
            try:
                task = EnumPRMCreator(
                    session=self._session,
                    tprm_instances=parameter_types,
                    parameters_by_object_id=new_prms,
                    in_case_of_error=ErrorHandlingType.PROCESS_CLEARED,
                    autocommit=False,
                )
                created_prms_with_errors = task.create_enum_parameters()

            except EnumValTypeCustomExceptions as e:
                raise HTTPException(
                    status_code=e.status_code,
                    detail=e.detail,
                )
            self._result_param_list.extend(
                created_prms_with_errors.created_parameters
            )
            self._result_error_list.extend(created_prms_with_errors.errors)

    def _process_common_val_types(self):
        if self._common_val_types:
            db_param_list, error_list = (
                db_create.create_parameters_with_error_list(
                    session=self._session,
                    params=self._object_parameters,
                    db_object=self._object_instance,
                )
            )
            self._result_param_list.extend(db_param_list)
            self._result_error_list.extend(error_list)

    def _process_parameters_by_val_type(self):
        for val_type, parameters in self._parameter_by_val_type.items():
            parameter_types = self._parameter_type_by_val_type[val_type]

            process_parameters_by_val_type = {
                two_way_mo_link_val_type_name: self._process_two_way_mo_link_parameters(
                    val_type=val_type,
                    parameters=parameters,
                    parameter_types=parameter_types,
                ),
                enum_val_type_name: self._process_enum_parameters(
                    val_type=val_type,
                    parameters=parameters,
                    parameter_types=parameter_types,
                ),
            }

            process_val_type_function = process_parameters_by_val_type.get(
                val_type,
                lambda: self._common_val_types.extend(self._object_parameters),
            )

            if process_val_type_function:
                process_val_type_function()

    def _response(self):
        if len(self._result_param_list) == 0:
            raise HTTPException(status_code=400, detail=self._result_error_list)

        else:
            self._session.commit()

            return CreateObjectParametersResponse(
                data=self._result_param_list, errors=self._result_error_list
            )

    def execute(self):
        self._collect_instances()
        self._process_parameters_by_val_type()
        self._process_common_val_types()

        return self._response()


class GetParameterData:
    def __init__(self, session: Session, parameter_ids: list[int]):
        self._session = session
        self._parameter_ids = parameter_ids

    def execute(self) -> list[ParameterData]:
        query = select(PRM, MO).join(MO).where(PRM.id.in_(self._parameter_ids))
        parameters = self._session.execute(query).scalars().all()
        result_parameters = []
        parameter_type_instances = {}
        for parameter_instance in parameters:
            if parameter_instance.tprm_id in parameter_type_instances:
                parameter_type_instance = parameter_type_instances[
                    parameter_instance.tprm_id
                ]
            else:
                parameter_type_instance = parameter_instance.tprm
                parameter_type_instances[parameter_instance.tprm_id] = (
                    parameter_type_instance
                )

            match parameter_type_instance.multiple:
                case True:
                    prm_values: list[str] = decode_pickle_data(
                        data=parameter_instance.value
                    )
                    match parameter_type_instance.val_type:
                        case "mo_link":
                            query = select(MO.name).where(MO.id.in_(prm_values))
                            value = self._session.execute(query).scalars().all()

                        case "prm_link":
                            query = select(PRM.value).where(
                                PRM.id.in_(prm_values)
                            )
                            value = self._session.execute(query).scalars().all()

                        case _:
                            value = prm_values

                case _:
                    match parameter_type_instance.val_type:
                        case "mo_link":
                            query = select(MO.name).where(
                                MO.id == int(parameter_instance.value)
                            )
                            value = self._session.execute(query).scalar()

                        case "prm_link":
                            query = select(PRM.value).where(
                                PRM.id == int(parameter_instance.value)
                            )
                            value = self._session.execute(query).scalar()

                        case _:
                            value = parameter_instance.value

            result_parameters.append(
                ParameterData(
                    mo_id=parameter_instance.mo.id,
                    prm_id=parameter_instance.id,
                    mo_name=parameter_instance.mo.name,
                    value=value,
                )
            )

        return result_parameters
