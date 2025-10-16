import copy
import dataclasses
import pickle
import time
from collections import defaultdict
from datetime import timezone, datetime
from typing import Any, List, Dict, Tuple, TypeAlias, Literal, Union

from fastapi import HTTPException
from sqlalchemy import cast, Integer, update, or_, tuple_
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import Session, select, and_

from common.common_constant import MO_LINK_DELIMITER, NAME_DELIMITER
from common.common_exceptions import ValidationError
from common.common_utils import ValueTypeValidator
from database import get_chunked_values_by_sqlalchemy_limit
from functions import functions_dicts
from functions.db_functions import db_create, db_read
from functions.db_functions.db_delete import delete_prm_links_by_prm_id
from functions.functions_utils.utils import (
    calculate_by_formula_new,
    extract_location_data,
    set_location_attrs,
    decode_multiple_value,
)
from functions.functions_utils.utils import (
    update_mo_label_when_update_label_prm,
)
from functions.validation_functions import validation_function
from models import PRM, TMO, MO, TPRM, GeometryType
from routers.object_router import utils as objects_utils
from routers.object_router.exceptions import ObjectNotExists
from routers.parameter_router.exceptions import (
    ParameterNotExists,
    ParameterTypeNotExists,
    PrimaryTPRMParameterError,
    ParametersAlreadyExistError,
    CannotCreateForSequenceTypeError,
    NotValidParameterVersion,
    NotValidSequenceValue,
)
from routers.parameter_router.processors import ParameterRepository
from routers.parameter_router.schemas import (
    DeleteParameter,
    PRMCreateByMO,
    PRMUpdateByMO,
    UpdateParameterByObject,
    CreateParameterByObject,
    NewParameterValue,
)
from val_types.constants import (
    two_way_mo_link_val_type_name,
    ErrorHandlingType,
    enum_val_type_name,
)
from val_types.enum_val_type.prm.create import EnumPRMCreator
from val_types.two_way_mo_link_val_type.prm.create import (
    create_two_way_mo_link_prms,
)
from val_types.two_way_mo_link_val_type.prm.delete import (
    delete_two_way_mo_link_prms,
)


def update_object_version_and_modification_date(
    session: Session, object_instance: MO
) -> None:
    # if object_instance:
    object_instance.version += 1
    object_instance.modification_date = datetime.now(timezone.utc)
    session.add(object_instance)


def update_param_validation(session, db_param_type, param_type):
    if db_param_type.version != param_type.version:
        raise HTTPException(
            status_code=409,
            detail=f"Actual version of TPRM: {db_param_type.version}.",
        )
    if db_param_type.required:
        raise HTTPException(
            status_code=409,
            detail="Parameter type is required. Not allowed to change val_type.",
        )
    validation_function.val_type_validation_when_update(param_type=param_type)
    if param_type.val_type == db_param_type.val_type:
        return db_param_type
    session.info["disable_security"] = True
    db_object_type = db_read.get_db_object_type_or_exception(
        session=session, object_type_id=db_param_type.tmo_id
    )
    if (
        db_param_type.id == db_object_type.latitude
        or db_param_type.id == db_object_type.longitude
    ):
        raise HTTPException(
            status_code=409,
            detail="Not allowed to change val_type for latitude and longitude.",
        )
    if db_param_type.id == db_object_type.status:
        raise HTTPException(
            status_code=409, detail="Not allowed to change val_type for status."
        )

    return True


def set_tmo_status_longitude_latitude_by_tprm_id_from_prm(
    session, db_param: PRM, db_mo: MO, db_tmo: TMO
):
    if db_tmo.longitude == db_param.tprm_id:
        db_mo.longitude = float(db_param.value)
        session.add(db_mo)

    if db_tmo.latitude == db_param.tprm_id:
        db_mo.latitude = float(db_param.value)
        session.add(db_mo)

    if db_tmo.status == db_param.tprm_id:
        db_mo.status = db_param.value
        session.add(db_mo)


def validate_param_type_if_required(session, param_type):
    field_value = (
        validation_function.field_value_is_not_none_for_required_validation(
            param_type=param_type
        )
    )

    try:
        validation_task = ValueTypeValidator(
            session=session,
            parameter_type_instance=param_type,
            value_to_validate=field_value,
        )
        validation_task.validate()
    except ValidationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    return field_value


def update_param_attributes(
    session, db_tmo: TMO, db_mo: MO, param_type_id: int, attribute: str
):
    if db_tmo.longitude == param_type_id:
        setattr(db_tmo, attribute, None)
        session.add(db_mo)


def update_child_prm_location(
    session: Session, prm_name: str, value: float, mo_child: list[MO]
):
    child_tmo: TMO = session.exec(
        select(TMO).where(TMO.id == mo_child[0].tmo_id)
    ).first()
    if not child_tmo.inherit_location:
        return
    else:
        for current_mo in mo_child:  # type: MO
            match prm_name:
                case "latitude":
                    current_mo.latitude = value
                    update_line(session, current_mo)
                case "longitude":
                    current_mo.longitude = value
                    update_line(session, current_mo)
                case _:
                    raise ValueError
            current_mo.version += 1
            session.add(current_mo)

            stmt = select(MO).where(MO.p_id == current_mo.id)
            mo_grandchild: list[MO] = session.exec(stmt).all()
            if mo_grandchild:
                update_child_prm_location(
                    session=session,
                    prm_name=prm_name,
                    value=value,
                    mo_child=mo_grandchild,
                )
        session.flush()


def create_prm_for_formula(
    session: Session, db_param_type: TPRM, mos: list[MO] | None = None
):
    if not mos:
        mos = _get_mo_for_formula(session=session, db_param_type=db_param_type)
    for cur_mo in mos:
        data: list[PRMCreateByMO] = [
            PRMCreateByMO(value=1, tprm_id=db_param_type.id)
        ]
        db_create.create_parameters_with_error_list(
            session=session, params=data, db_object=cur_mo
        )


def update_prm_for_formula(
    session: Session, db_param_type: TPRM, mos: list[MO] | None = None
):
    if db_param_type.constraint:
        if db_param_type.constraint.find("INNER_MAX['") != -1:
            return
        if not mos:
            mos = _get_mo_for_formula(
                session=session, db_param_type=db_param_type
            )
        for cur_mo in mos:
            start_time = time.perf_counter()
            try:
                value = calculate_by_formula_new(
                    session=session,
                    param_type=db_param_type,
                    object_instance=cur_mo,
                )
            except ValueError as ex:
                raise ValueError(ex.args)
            end_time = time.perf_counter()
            print(
                f"Update PRM for formula calculate new PRM value: {end_time - start_time}"
            )
            start_time = time.perf_counter()
            for prm in cur_mo.prms:
                if prm.tprm_id == db_param_type.id:
                    prm.value = value
                    prm.version += 1
                    session.add(prm)
                    break
            else:
                db_param = PRM(
                    tprm_id=db_param_type.id, mo_id=cur_mo.id, value=value
                )
                session.add(db_param)
                session.flush()
            update_object_version_and_modification_date(
                session=session, object_instance=cur_mo
            )

        session.commit()


def _get_mo_for_formula(session: Session, db_param_type: TPRM) -> list[MO]:
    tprm_names: list[str] = functions_dicts.extract_formula_parameters(
        formula=db_param_type.constraint
    )
    if tprm_names:
        stmt = (
            select(MO)
            .join_from(MO, TPRM, MO.tmo_id == TPRM.tmo_id)
            .join_from(
                MO, PRM, and_(PRM.mo_id == MO.id, TPRM.id == PRM.tprm_id)
            )
            .where(MO.tmo_id == db_param_type.tmo_id, TPRM.name.in_(tprm_names))
            .distinct(MO.id)
        )
    else:
        stmt = (
            select(MO)
            .join_from(MO, TPRM, MO.tmo_id == TPRM.tmo_id)
            .join_from(
                MO, PRM, and_(PRM.mo_id == MO.id, TPRM.id == PRM.tprm_id)
            )
            .where(MO.tmo_id == db_param_type.tmo_id)
            .distinct(MO.id)
        )
    return session.exec(stmt).all()


def update_line(session: Session, db_mo: MO) -> None:
    stmt = select(MO).where(
        and_(MO.point_a_id == db_mo.id, MO.point_b_id.isnot(None))
    )
    mo_for_update = session.exec(stmt).all()
    for mo in mo_for_update:
        mo.geometry = objects_utils.update_geometry(mo, db_mo, mo.point_b)
        flag_modified(mo, "geometry")
        session.add(mo)

    session.flush()
    stmt = select(MO).where(
        and_(MO.point_b_id == db_mo.id, MO.point_a_id.isnot(None))
    )
    mo_for_update = session.exec(stmt).all()
    for mo in mo_for_update:
        mo.geometry = objects_utils.update_geometry(mo, mo.point_a, db_mo)
        flag_modified(mo, "geometry")
        session.add(mo)

    session.flush()


def update_sequence(
    session: Session, new_param: PRMUpdateByMO, old_param: PRM, param_type: TPRM
):
    """
    Function that update all values in a sequence
    :param session: DB session instance
    :param new_param: new PRM instance
    :param old_param: old PRM instance
    :param param_type: TPRM instance
    """
    if new_param.value == old_param.value:
        return

    # action to do with sequence: if value 'increased' -> collapse else -> shift
    increased = int(new_param.value) > int(old_param.value)

    query = select(PRM).where(PRM.tprm_id == param_type.id)
    if increased:
        query = query.where(
            cast(PRM.value, Integer) <= int(new_param.value),
            cast(PRM.value, Integer) > int(old_param.value),
        )
    else:
        query = query.where(
            cast(PRM.value, Integer) >= int(new_param.value),
            cast(PRM.value, Integer) < int(old_param.value),
        )

    # if sequence depends on another TPRM -> add filter by MO with same dependency
    if param_type.constraint:
        type_subquery = (
            select(PRM.value)
            .where(
                PRM.mo_id == old_param.mo_id,
                PRM.tprm_id == int(param_type.constraint),
            )
            .scalar_subquery()
        )
        mo_subquery = (
            select(PRM.mo_id)
            .where(
                PRM.tprm_id == int(param_type.constraint),
                PRM.value == type_subquery,
            )
            .scalar_subquery()
        )
        query = query.where(PRM.mo_id.in_(mo_subquery))

    query = query.execution_options(yield_per=1000)
    # update values in selected parameters
    params_to_update = session.execute(query)
    params_to_update = params_to_update.scalars().partitions(1000)
    for chunk in params_to_update:
        for param_to_update in chunk:
            if increased:
                new_value = int(param_to_update.value) - 1
            else:
                new_value = int(param_to_update.value) + 1

            query = (
                update(PRM)
                .values(value=str(new_value))
                .where(PRM.id == param_to_update.id)
            )
            session.execute(query)


def collapse_sequence_after_update_constraint(
    session: Session, tprm: TPRM, sequence_type: Any, position: str
):
    subquery = select(PRM.mo_id).where(
        PRM.value == sequence_type, PRM.tprm_id == int(tprm.constraint)
    )
    query = (
        select(PRM)
        .where(
            PRM.tprm_id == tprm.id,
            PRM.mo_id.in_(subquery),
            cast(PRM.value, Integer) >= int(position),
        )
        .execution_options(yield_per=1000)
    )
    chunks = session.exec(query)
    for chunk in chunks.partitions(1000):
        for prm in chunk:
            prm.value = str(int(prm.value) - 1)
            session.add(prm)


def update_depending_sequences_after_update_constraint(
    session: Session,
    sequences: list[TPRM],
    old_sequence_type: Any,
    new_sequence_type: Any,
    mo_id: int,
):
    query = select(PRM.tprm_id, PRM.value).where(
        PRM.mo_id == mo_id, PRM.tprm_id.in_([seq.id for seq in sequences])
    )
    positions = session.execute(query)
    positions = {pos[0]: pos[1] for pos in positions.all()}
    for sequence in sequences:
        collapse_sequence_after_update_constraint(
            session=session,
            tprm=sequence,
            sequence_type=old_sequence_type,
            position=positions[sequence.id],
        )
        new_value = objects_utils.get_value_for_sequence(
            session=session,
            param_type_info=sequence,
            sequence_type=new_sequence_type,
        )
        session.execute(query)

        stmt = select(PRM).where(PRM.mo_id == mo_id, PRM.tprm_id == sequence.id)
        param_to_update = session.exec(stmt).first()
        param_to_update.value = str(new_value - 1)
        session.add(param_to_update)

    session.commit()


def get_mo_link_real_value(session: Session, tprm: TPRM, value: str):
    query = select(MO.id)

    if value.__contains__(MO_LINK_DELIMITER):
        value = value.split(MO_LINK_DELIMITER)
        tmo_subquery = (
            select(TMO.id).where(TMO.name == value[0]).scalar_subquery()
        )
        query = query.where(MO.tmo_id == tmo_subquery, MO.name == value[1])
    else:
        query = query.where(MO.name == value)

    if tprm.constraint:
        query = query.where(MO.tmo_id == int(tprm.constraint))

    real_value = session.execute(query).scalar()

    if real_value is None:
        print(f"link by name is None: {tprm.id}-{value}")
        if str(value).isdigit():
            query = select(MO.id).where(MO.id == int(value))
            real_value = session.execute(query)
            real_value = real_value.scalar()

    return real_value


def get_mo_link_full_name(session: Session, tprm: TPRM, value: str):
    if value.isdigit():
        query = select(MO.name, MO.tmo_id).where(MO.name == value)
        if tprm.constraint:
            query = query.where(MO.tmo_id == int(tprm.constraint))

        response = session.execute(query)
        response = [
            {"name": res[0], "tmo_id": res[1]} for res in response.fetchall()
        ]
        if not response:
            query = select(MO.name, MO.tmo_id).where(MO.id == int(value))
            response = session.execute(query)
            response = [
                {"name": res[0], "tmo_id": res[1]}
                for res in response.fetchall()
            ]

        response = response[0]
        query = select(TMO.name).where(TMO.id == response["tmo_id"])
        tmo_name = session.execute(query).scalar()
        response["tmo_name"] = tmo_name
    else:
        if value.__contains__(MO_LINK_DELIMITER):
            return value

        if tprm.constraint:
            query = select(TMO.name).where(TMO.id == int(tprm.constraint))
            res = session.execute(query).scalar()
        else:
            subquery = (
                select(MO.tmo_id).where(MO.name == value).scalar_subquery()
            )
            query = select(TMO.name).where(TMO.id == subquery)
            res = session.execute(query).scalar()
        response = {"name": value, "tmo_name": res}

    return f"{response['tmo_name']}{MO_LINK_DELIMITER}{response['name']}"


@dataclasses.dataclass
class ParameterToDelete:
    tprm_instance: TPRM
    object_id: int
    parameter_to_delete: PRM


class MassiveDeleteParameters:
    def __init__(
        self, data_for_delete: List[DeleteParameter], session: Session
    ):
        self.__requested_data_for_delete: List[DeleteParameter] = (
            data_for_delete
        )
        self._session: Session = session
        self._parameters_to_delete: List[ParameterToDelete] = []

        self._deleted_parameters: List[PRM] = []

    def _validate_exists_parameters(self):
        mo_and_tprm_id_pair = set()
        for requested_instance in self.__requested_data_for_delete:
            mo_and_tprm_id_pair.add(
                (requested_instance.object_id, requested_instance.tprm_id)
            )

        self.__parameter_instances: List[PRM] = (
            self._session.query(PRM)
            .filter(tuple_(PRM.mo_id, PRM.tprm_id).in_(mo_and_tprm_id_pair))
            .all()
        )

        exists_mo_and_tprm_id_pair = {
            (parameter.mo_id, parameter.tprm_id)
            for parameter in self.__parameter_instances
        }

        not_exists_parameters = mo_and_tprm_id_pair.difference(
            exists_mo_and_tprm_id_pair
        )

        if not_exists_parameters:
            detail_not_exists_pairs = [
                {"object_id": object_id, "tprm_id": tprm_id}
                for object_id, tprm_id in not_exists_parameters
            ]
            raise ParameterNotExists(
                status_code=422,
                detail=f"There are parameters, which are not exists: {detail_not_exists_pairs}",
            )

    def _collect_tprm_instances(self):
        tprm_ids = {
            requested_instance.tprm_id
            for requested_instance in self.__requested_data_for_delete
        }

        self.__tprm_instances_by_id = {}
        for chunk in get_chunked_values_by_sqlalchemy_limit(tprm_ids):
            exists_tprms = self._session.execute(
                select(TPRM).where(TPRM.id.in_(chunk))
            ).scalars()
            self.__tprm_instances_by_id.update(
                {tprm.id: tprm for tprm in exists_tprms}
            )

    def _collect_data_for_delete(self):
        for parameter_instance in self.__parameter_instances:
            tprm_instance = self.__tprm_instances_by_id[
                parameter_instance.tprm_id
            ]

            self._parameters_to_delete.append(
                ParameterToDelete(
                    tprm_instance=tprm_instance,
                    object_id=parameter_instance.mo_id,
                    parameter_to_delete=parameter_instance,
                )
            )

    def execute(self):
        self._validate_exists_parameters()

        self._collect_tprm_instances()
        self._collect_data_for_delete()

        objects_to_update = set()

        for parameter_to_delete in self._parameters_to_delete:
            delete_parameter_instance(
                tprm=parameter_to_delete.tprm_instance,
                session=self._session,
                param=parameter_to_delete.parameter_to_delete,
                object_id=parameter_to_delete.object_id,
            )
            self._deleted_parameters.append(
                parameter_to_delete.parameter_to_delete
            )
            objects_to_update.add(parameter_to_delete.object_id)

        for object_id in objects_to_update:
            db_mo = self._session.get(MO, object_id)
            update_object_version_and_modification_date(
                session=self._session, object_instance=db_mo
            )

        self._session.commit()
        return self._deleted_parameters


class MassiveUpdateParameters:
    def __init__(
        self, data_for_update: List[UpdateParameterByObject], session: Session
    ):
        self.data_for_update = data_for_update
        self.session = session

        # CONSTANTS
        self.__sequence_params = []
        self.__formula_params = []
        self.__tprms = []
        self.__objects = []
        self._result_updated_params = []
        self.__tmos_and_sequence_values = {}
        self.__tprms_by_tmos = defaultdict(list)
        self.__objects_by_tmos = defaultdict(list)
        self._tprm_id_and_instance = {}
        self._tmo_id_and_instance: dict[int, TMO] = {}
        self._mo_id_and_instance: dict[int, MO] = {}
        self.__mos_and_prms: dict[int, list[PRM]] = defaultdict(list)
        self._mos_and_prms_by_tprm: dict[int, dict[int, PRM]] = {}

    def _get_tprm_instances(self):
        """
        This method collect tprm_ids from request and get TPRM instaces from db by collected tprm_ids.
        It returns nothing, but gather data
        """
        all_tprm_ids = {
            new_value.tprm_id
            for obj_for_update in self.data_for_update
            for new_value in obj_for_update.new_values
        }

        for chunk in get_chunked_values_by_sqlalchemy_limit(
            values=all_tprm_ids
        ):
            self.__tprms.extend(
                self.session.execute(select(TPRM).where(TPRM.id.in_(chunk)))
                .scalars()
                .all()
            )

        for tprm in self.__tprms:
            self._tprm_id_and_instance[tprm.id] = tprm
            self.__tprms_by_tmos[tprm.tmo_id].append(tprm.id)

    def _get_tmo_instances(self):
        """
        This method get used tmos from request, by getting link "TMO_ID - TPRM INSTANCE" and
        create link "TMO_ID - TMO INSTANCE"
        """
        for tmo_ids in get_chunked_values_by_sqlalchemy_limit(
            self.__tprms_by_tmos.keys()
        ):
            query = (
                self.session.execute(select(TMO).where(TMO.id.in_(tmo_ids)))
                .scalars()
                .all()
            )
            self._tmo_id_and_instance.update({tmo.id: tmo for tmo in query})

    def _get_mo_instances(self):
        """
        This method collect mo_ids from request and get MO instaces from db by collected mo_ids.
        So in result it create link "MO ID - MO INSTANCE" and link  "SPECIFIC_TMO_ID - MOS_OF_SPECIFIC_TMO"
        It returns nothing, but gather data
        """
        all_obj_ids = {
            obj_for_update.object_id for obj_for_update in self.data_for_update
        }

        for chunk in get_chunked_values_by_sqlalchemy_limit(values=all_obj_ids):
            self.__objects.extend(
                self.session.execute(select(MO).where(MO.id.in_(chunk)))
                .scalars()
                .all()
            )

        for obj in self.__objects:
            self._mo_id_and_instance[obj.id] = obj
            self.__objects_by_tmos[obj.tmo_id].append(obj.id)

    def _get_prms_by_mo_id(self):
        """
        This method created link "SPECIFIC_MO_ID - PRMs_OF_SPECIFIC_MO"
        """
        for tmo_id, object_ids in self.__objects_by_tmos.items():
            tprms_of_current_tmo = self.__tprms_by_tmos[tmo_id]
            for mo_ids_chunk in get_chunked_values_by_sqlalchemy_limit(
                object_ids
            ):
                for tprm_ids_chunk in get_chunked_values_by_sqlalchemy_limit(
                    tprms_of_current_tmo
                ):
                    stmt = select(PRM).where(
                        PRM.mo_id.in_(mo_ids_chunk),
                        PRM.tprm_id.in_(tprm_ids_chunk),
                    )

                    for prm in self.session.execute(stmt).scalars().all():
                        self.__mos_and_prms[prm.mo_id].append(prm)

    def _map_mos_and_params_by_tprm(self):
        """
        This method add tprm id to link "SPECIFIC_MO_ID - PRMs_OF_SPECIFIC_MO",

        so after that we will have "SPECIFIC_MO_ID - TPRMs_OF_SPECIFIC_MO - PRMs_OF_SPECIFIC_TPRM"
        """
        for mo_id, params in self.__mos_and_prms.items():
            new_formatted_data = {}
            for param in params:
                new_formatted_data[param.tprm_id] = param

            self._mos_and_prms_by_tprm[mo_id] = new_formatted_data

    def _check_version_of_updated_parameters(self):
        """
        When user take request we need to provide integrity for data: version to version
        """
        for object_inst in self.data_for_update:  # type: UpdateParameterByObject
            for value in object_inst.new_values:  # type: NewParameterValue
                if value.version:
                    version_in_db = (
                        self._mos_and_prms_by_tprm.get(object_inst.object_id)
                        .get(value.tprm_id)
                        .version
                    )
                    if value.version != version_in_db:
                        raise NotValidParameterVersion(
                            status_code=422,
                            detail=(
                                f"Objects with ids: {object_inst.object_id} and tprm id: "
                                f"{value.tprm_id} must have last version!"
                            ),
                        )

    def __collect_sequence_parameter(
        self,
        session: Session,
        current_tprm: TPRM,
        value: NewParameterValue,
        object_instance: MO,
        current_parameter: PRM,
    ):
        """
        We need to update sequence after all other params, because this type can
        be linked to new updated params
        """
        sequence_values = self.__tmos_and_sequence_values.get(
            object_instance.tmo_id, []
        )

        # values of sequences can't be equals in the same TMO space
        if value.new_value in sequence_values:
            raise NotValidSequenceValue(
                status_code=422,
                detail="Objects in the same TMO can't have same sequence value.",
            )
        else:
            if sequence_values:
                sequence_values.append(value.new_value)
            else:
                self.__tmos_and_sequence_values[object_instance.tmo_id] = [
                    sequence_values
                ]

        stmt = select(PRM.value).where(
            PRM.tprm_id == int(current_tprm.constraint),
            PRM.mo_id == object_instance.id,
        )
        sequence_type = session.execute(stmt).scalar()

        objects_utils.validate_sequence_value_if_constraint(
            session=session,
            param_type_info=self._tprm_id_and_instance[value.tprm_id],
            sequence_type=sequence_type,
            user_value=value.new_value,
        )

        self.__sequence_params.append(
            {
                "new_param": PRMUpdateByMO(
                    tprm_id=value.tprm_id, value=value.new_value
                ),
                "old_param": current_parameter,
                "param_type": self._tprm_id_and_instance[value.tprm_id],
            }
        )

    def __collect_formula_parameter(
        self,
        current_parameter: PRM,
        value: NewParameterValue,
        object_instance: MO,
    ):
        """
        We need to update formula after all other params, because this type can
        be linked to new updated params
        """
        self.__formula_params.append(
            {
                "param_instance": current_parameter,
                "param_type": self._tprm_id_and_instance[value.tprm_id],
                "db_mo": object_instance,
                "param_value": value.new_value,
            }
        )

    @staticmethod
    def _set_mo_attributes_which_linked_to_parameter_values(
        session: Session,
        current_tprm: TPRM,
        current_tmo: TMO,
        value: NewParameterValue,
        object_instance: MO,
    ):
        """
        MO has attributes, which depend on parameter values: latitude, longitude, status.
        So if user update parameter, which link to one of these attributes - we need to update attribute
        """
        children_of_current_object = object_instance.children

        if current_tprm.id == current_tmo.latitude:
            object_instance.latitude = float(value.new_value)
            session.add(object_instance)

            if children_of_current_object:
                update_child_prm_location(
                    session=session,
                    prm_name="latitude",
                    value=float(value.new_value),
                    mo_child=children_of_current_object,
                )

        elif current_tprm.id == current_tmo.longitude:
            object_instance.longitude = float(value.new_value)
            session.add(object_instance)

            if children_of_current_object:
                update_child_prm_location(
                    session=session,
                    prm_name="longitude",
                    value=float(value.new_value),
                    mo_child=children_of_current_object,
                )

        elif current_tprm.id == current_tmo.status:
            object_instance.status = value.new_value
            session.add(object_instance)

    @staticmethod
    def _update_inherit_location_for_child_objects(
        session: Session, current_tmo: TMO, object_instance: MO
    ):
        inherit_can_be_changed = (
            current_tmo.longitude or current_tmo.latitude
        ) and current_tmo.geometry_type
        if inherit_can_be_changed:
            stmt = (
                select(MO)
                .join(TMO)
                .where(
                    MO.p_id == object_instance.id,
                    TMO.inherit_location.is_(True),
                )
            )
            child_object_with_inherit_location = (
                session.execute(stmt).scalars().all()
            )

            geometry_type = getattr(GeometryType, current_tmo.geometry_type)

            location_data = extract_location_data(
                geometry_type=geometry_type, parent_mo=object_instance
            )

            set_location_attrs(
                session=session,
                db_param=geometry_type,
                child_mos=child_object_with_inherit_location,
                set_value=True,
                location_data=location_data,
            )

    def _set_new_names_for_object_by_updated_primary_values(self):
        """
        MO name can be gathered from values of TPRM, which in primary list. So if we update primary
        parameter -- we need to update object names, which depends on this parameter
        """
        for object_id, tprm_and_params in self._mos_and_prms_by_tprm.items():
            object_instance = self._mo_id_and_instance[object_id]
            objects_type_instance = self._tmo_id_and_instance[
                object_instance.tmo_id
            ]
            primary_tprms = objects_type_instance.primary

            add_parent_name = (
                not objects_type_instance.global_uniqueness
                and objects_type_instance.p_id
            )
            if primary_tprms:
                new_name_parts = []
                if add_parent_name and object_instance.p_id:
                    parent_name = self.session.get(
                        MO, object_instance.p_id
                    ).name
                    new_name_parts.insert(0, parent_name)

                for primary_tprm in primary_tprms:
                    query = select(PRM.value).where(
                        PRM.tprm_id == int(primary_tprm),
                        PRM.mo_id == object_instance.id,
                    )
                    parameter_value = self.session.execute(query).scalar()
                    parameter_type_instance_val_type = self.session.get(
                        TPRM, int(primary_tprm)
                    ).val_type
                    if parameter_type_instance_val_type == "mo_link":
                        parameter_value = self.session.get(
                            MO, int(parameter_value)
                        ).name
                    if parameter_type_instance_val_type == "prm_link":
                        parameter_value = self.session.get(
                            PRM, int(parameter_value)
                        ).value

                    new_name_parts.append(parameter_value)
                new_name = NAME_DELIMITER.join(new_name_parts)

            else:
                new_name = str(object_instance.id)

            if object_instance.name != new_name:
                object_instance.name = new_name
                self.session.add(object_instance)

    def _update_parameters(self):
        objects_to_update = set()

        for object_inst in self.data_for_update:
            object_instance: MO = self._mo_id_and_instance[
                object_inst.object_id
            ]
            objects_to_update.add(object_inst.object_id)

            for value in object_inst.new_values:
                tprms_of_mo = self._mos_and_prms_by_tprm.get(
                    object_inst.object_id
                )
                current_parameter = (
                    tprms_of_mo.get(value.tprm_id) if tprms_of_mo else None
                )

                if not current_parameter:
                    raise ParameterNotExists(
                        status_code=422,
                        detail=f"Parameter for object with id: {object_inst.object_id}, "
                        f"and tprm with id: {value.tprm_id} doesn't exists",
                    )

                current_tprm = self._tprm_id_and_instance.get(value.tprm_id)

                try:
                    validation_task = ValueTypeValidator(
                        session=self.session,
                        parameter_type_instance=current_tprm,
                        value_to_validate=value.new_value,
                    )
                    validation_task.validate()

                except ValidationError:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Parameter {value.new_value} for parameter type with id {value.tprm_id} not valid",
                    )
                if (
                    current_tprm.val_type == "sequence"
                    and current_tprm.constraint
                ):
                    self.__collect_sequence_parameter(
                        session=self.session,
                        current_tprm=current_tprm,
                        value=value,
                        object_instance=object_instance,
                        current_parameter=current_parameter,
                    )
                    continue

                if current_tprm.val_type == "formula":
                    self.__collect_formula_parameter(
                        current_parameter=current_parameter,
                        value=value,
                        object_instance=object_instance,
                    )
                    continue

                # if success validation, we can save new value
                current_parameter.value = str(value.new_value)
                if current_tprm.multiple:
                    current_parameter.value = pickle.dumps(
                        value.new_value
                    ).hex()
                current_parameter.version = current_parameter.version + 1
                self.session.add(current_parameter)

                self._result_updated_params.append(current_parameter)

                current_tmo = self._tmo_id_and_instance[object_instance.tmo_id]

                self._set_mo_attributes_which_linked_to_parameter_values(
                    session=self.session,
                    current_tprm=current_tprm,
                    current_tmo=current_tmo,
                    value=value,
                    object_instance=object_instance,
                )

                self._update_inherit_location_for_child_objects(
                    session=self.session,
                    current_tmo=current_tmo,
                    object_instance=object_instance,
                )
                if value.tprm_id in current_tprm.tmo.label:
                    update_mo_label_when_update_label_prm(
                        session=self.session,
                        db_object=object_instance,
                        db_object_type=current_tprm.tmo,
                    )

        for object_id in objects_to_update:
            db_mo = self._mo_id_and_instance[object_id]
            update_object_version_and_modification_date(
                session=self.session, object_instance=db_mo
            )

    def _update_formula_parameters(self):
        for formula in self.__formula_params:
            updated_param = formula["param_instance"]
            updated_param.value = calculate_by_formula_new(
                session=self.session,
                param_type=formula["param_type"],
                object_instance=formula["db_mo"],
                x=formula["param_value"],
            )
            updated_param.version = updated_param.version + 1

            self.session.add(updated_param)
            self._result_updated_params.append(updated_param)

    def _update_sequence_parameters(self):
        for sequence in self.__sequence_params:
            update_sequence(
                session=self.session,
                new_param=sequence["new_param"],
                old_param=sequence["old_param"],
                param_type=sequence["param_type"],
            )
            updated_param = sequence["old_param"]
            updated_param.value = sequence["new_param"].value
            updated_param.version = updated_param.version + 1

            self.session.add(updated_param)
            self._result_updated_params.append(updated_param)

    def _calculate_geometry_for_linked_objects_to_updated_parameters(self):
        # because session.commit will empty objects in 'result_updated_params'
        # we need to create deep copy
        self._response = copy.deepcopy(self._result_updated_params)
        already_checked_objects = set()
        for parameter in self._response:
            if parameter.mo_id in already_checked_objects:
                continue

            already_checked_objects.add(parameter.mo_id)

            current_mo = self._mo_id_and_instance.get(
                parameter.mo_id, self.session.get(MO, parameter.mo_id)
            )
            current_tmo = self._tmo_id_and_instance.get(
                current_mo.tmo_id, self.session.get(TMO, current_mo.tmo_id)
            )

            if current_tmo.geometry_type == GeometryType.point.value:
                stmt = select(MO).where(
                    or_(
                        MO.point_a_id == current_mo.id,
                        MO.point_b_id == current_mo.id,
                    )
                )
                line_mos = self.session.exec(stmt).all()

                for line_mo in line_mos:
                    if (
                        current_mo.latitude is None
                        or current_mo.longitude is None
                    ):
                        line_mo.geometry = {}
                        self.session.add(line_mo)

                    else:
                        if (
                            current_mo.id == line_mo.point_a_id
                            and line_mo.point_b_id is not None
                        ):
                            point_b_object = self.session.exec(
                                select(MO).where(MO.id == line_mo.point_b_id)
                            ).first()
                            if (
                                point_b_object.latitude is not None
                                and point_b_object.longitude is not None
                            ):
                                line_mo.geometry = (
                                    objects_utils.update_geometry(
                                        object_instance=line_mo,
                                        point_a=current_mo,
                                        point_b=point_b_object,
                                    )
                                )
                            else:
                                line_mo.geometry = None
                            self.session.add(line_mo)

                        elif (
                            current_mo.id == line_mo.point_b_id
                            and line_mo.point_a_id is not None
                        ):
                            point_a_object = self.session.exec(
                                select(MO).where(MO.id == line_mo.point_a_id)
                            ).first()
                            if (
                                point_a_object.latitude is not None
                                and point_a_object.longitude is not None
                            ):
                                line_mo.geometry = (
                                    objects_utils.update_geometry(
                                        object_instance=line_mo,
                                        point_a=point_a_object,
                                        point_b=current_mo,
                                    )
                                )

                            else:
                                line_mo.geometry = None

                            line_mo.version = line_mo.version + 1

                            flag_modified(line_mo, key="geometry")

                            self.session.add(line_mo)

    def _collect_data_instances(self):
        self._get_tprm_instances()
        self._get_tmo_instances()
        self._get_mo_instances()

        self._get_prms_by_mo_id()
        self._map_mos_and_params_by_tprm()

    def check(self):
        self._collect_data_instances()
        self._check_version_of_updated_parameters()

    def execute(self):
        self._update_parameters()
        self.session.flush()

        self._update_formula_parameters()
        self._update_sequence_parameters()
        self._calculate_geometry_for_linked_objects_to_updated_parameters()
        self.session.flush()
        self._set_new_names_for_object_by_updated_primary_values()

        self.session.commit()

        return self._response


IdType: TypeAlias = int


class MultipleCreateParameterService:
    def __init__(
        self, data_for_create: List[CreateParameterByObject], session: Session
    ) -> None:
        self.data_for_create = data_for_create
        self.session = session
        self.repository = ParameterRepository(session)

        self.__tprm_instances: List[TPRM] = list()
        self.__object_instances: List[MO] = list()
        self.__mo_id_and_instance: Dict[IdType, MO] = dict()
        self.__tprm_id_and_instance: Dict[IdType, TPRM] = dict()
        self.__tmo_id_and_instance: Dict[IdType, TMO] = dict()
        self.__objects_by_tmos: defaultdict[IdType, List[IdType]] = defaultdict(
            list
        )
        self.__tprms_by_tmos: defaultdict[IdType, List[IdType]] = defaultdict(
            list
        )
        self.__prepared_params_for_create: List[PRM] = list()
        self.__formula_params: List[Dict[str, Any]] = list()

    def _get_mo_instances(self) -> None:
        requested_object_ids = {
            prms_and_obj_id.object_id
            for prms_and_obj_id in self.data_for_create
        }
        self.__object_instances = self.repository.get_mos_by_ids(
            list(requested_object_ids)
        )

        for obj in self.__object_instances:
            self.__mo_id_and_instance[obj.id] = obj
            self.__objects_by_tmos[obj.tmo_id].append(obj.id)

        not_exists_objects = requested_object_ids - set(
            self.__mo_id_and_instance.keys()
        )
        if not_exists_objects:
            raise ObjectNotExists(
                status_code=422,
                detail=f"MO(s) with id(s) {list(not_exists_objects)} do not exist",
            )

    def _get_tprm_instances(self) -> None:
        requested_tprm_ids = {
            new_value.tprm_id
            for prms_and_obj_id in self.data_for_create
            for new_value in prms_and_obj_id.new_values
        }

        self.__tprm_instances = self.repository.get_tprms_by_ids(
            list(requested_tprm_ids)
        )

        for tprm in self.__tprm_instances:
            self.__tprm_id_and_instance[tprm.id] = tprm
            self.__tprms_by_tmos[tprm.tmo_id].append(tprm.id)

        not_exists_tprm_ids = requested_tprm_ids - set(
            self.__tprm_id_and_instance.keys()
        )
        if not_exists_tprm_ids:
            raise ParameterTypeNotExists(
                status_code=422,
                detail=f"TPRM(s) with id(s) {list(not_exists_tprm_ids)} do not exist",
            )

        tmo_ids_by_requested_tprms = list(self.__tprms_by_tmos.keys())
        tmo_instances = (
            self.session.execute(
                select(TMO).where(TMO.id.in_(tmo_ids_by_requested_tprms))
            )
            .scalars()
            .all()
        )
        for tmo_instance in tmo_instances:
            self.__tmo_id_and_instance[tmo_instance.id] = tmo_instance

        for tmo_id, tprm_ids in self.__tprms_by_tmos.items():
            tmo_instance = self.__tmo_id_and_instance[tmo_id]
            for tprm_id in tprm_ids:
                if tprm_id in tmo_instance.primary:
                    raise PrimaryTPRMParameterError(
                        status_code=422,
                        detail=f"Can't create parameter for tprm with id: {tprm_id}, "
                        f"because it`s primary tprm.",
                    )

    def _check_parameters_for_existence(self) -> None:
        for tmo_id, object_ids in self.__objects_by_tmos.items():
            tprm_ids_of_current_tmo = self.__tprms_by_tmos[tmo_id]

            already_created_params: List[Tuple[IdType, IdType]] = (
                self.repository.get_params_by_mo_and_tprm_ids(
                    mo_ids=object_ids, tprm_ids=tprm_ids_of_current_tmo
                )
            )

            if already_created_params:
                created_params = [
                    f"{mo_id}-{tprm_id}"
                    for mo_id, tprm_id in already_created_params
                ]
                raise ParametersAlreadyExistError(
                    status_code=422,
                    detail=f"There are parameters, which already exist: with mo_id-tprm_id pairs: {created_params}.",
                )

    def _create_parameter(
        self, object_with_requested_parameters: CreateParameterByObject
    ) -> None:
        object_instance: MO = self.__mo_id_and_instance.get(
            object_with_requested_parameters.object_id
        )

        for value_to_create in object_with_requested_parameters.new_values:
            tprm_instance: TPRM = self.__tprm_id_and_instance.get(
                value_to_create.tprm_id
            )

            if tprm_instance.val_type == two_way_mo_link_val_type_name:
                self._handle_two_way_mo_link(
                    tprm_instance=tprm_instance,
                    value_to_create=value_to_create,
                    object_instance=object_instance,
                )

            elif tprm_instance.val_type == enum_val_type_name:
                self._handle_enum(
                    tprm_instance=tprm_instance,
                    value_to_create=value_to_create,
                    object_instance=object_instance,
                )

            else:
                if tprm_instance.val_type == "sequence":
                    raise CannotCreateForSequenceTypeError(
                        status_code=422,
                        detail=f"Can't create parameter for tprm with id "
                        f"{tprm_instance.id}, because its val type is sequence.",
                    )

                if tprm_instance.val_type == "formula":
                    self._handle_formula_parameter(
                        object_instance=object_instance, value=value_to_create
                    )
                    continue
                try:
                    validation_task = ValueTypeValidator(
                        session=self.session,
                        parameter_type_instance=tprm_instance,
                        value_to_validate=value_to_create.new_value,
                    )
                    validation_task.validate()
                except ValidationError:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Parameter {value_to_create.new_value} for parameter type with id "
                        f"{value_to_create.tprm_id} does not valid",
                    )
                self._save_new_parameter(
                    value_to_create=value_to_create,
                    object_instance=object_instance,
                )

            self._update_mo(
                object_instance=object_instance,
                tprm_instance=tprm_instance,
                value_to_create=value_to_create,
            )

    def _handle_two_way_mo_link(
        self,
        tprm_instance: TPRM,
        object_instance: MO,
        value_to_create: NewParameterValue,
    ) -> None:
        new_prms = {
            object_instance.id: [
                PRMCreateByMO(
                    value=value_to_create.new_value,
                    tprm_id=value_to_create.tprm_id,
                )
            ]
        }
        errors, created_prms = create_two_way_mo_link_prms(
            session=self.session,
            parameter_types=[tprm_instance],
            autocommit=False,
            in_case_of_error=ErrorHandlingType.RAISE_ERROR,
            new_parameter_types=new_prms,
        )
        self.__prepared_params_for_create.extend(created_prms)

    def _handle_enum(
        self,
        tprm_instance: TPRM,
        object_instance: MO,
        value_to_create: NewParameterValue,
    ) -> None:
        new_prms = {
            object_instance.id: [
                PRMCreateByMO(
                    value=value_to_create.new_value,
                    tprm_id=value_to_create.tprm_id,
                )
            ]
        }
        task = EnumPRMCreator(
            session=self.session,
            tprm_instances=[tprm_instance],
            parameters_by_object_id=new_prms,
            in_case_of_error=ErrorHandlingType.RAISE_ERROR,
            autocommit=False,
        )
        created_prms_with_errors = task.create_enum_parameters()
        self.__prepared_params_for_create.extend(
            created_prms_with_errors.created_parameters
        )

    def _handle_formula_parameter(
        self, object_instance: MO, value: NewParameterValue
    ) -> None:
        parameter_for_db = PRM(
            mo_id=object_instance.id,
            tprm_id=value.tprm_id,
            value=value.new_value,
            version=1,
        )
        self.__formula_params.append(
            {
                "param_instance": parameter_for_db,
                "param_type": self.__tprm_id_and_instance[value.tprm_id],
                "db_mo": object_instance,
                "param_value": value.new_value,
            }
        )

    def _save_new_parameter(
        self, value_to_create: NewParameterValue, object_instance: MO
    ) -> None:
        new_value = value_to_create.new_value
        if isinstance(new_value, list):
            new_value = pickle.dumps(new_value).hex()

        parameter_for_db = PRM(
            mo_id=object_instance.id,
            tprm_id=value_to_create.tprm_id,
            value=new_value,
            version=1,
        )

        self.repository.add_prm_to_session(prm=parameter_for_db)
        self.__prepared_params_for_create.append(parameter_for_db)

    def _update_mo(
        self,
        object_instance: MO,
        tprm_instance: TPRM,
        value_to_create: NewParameterValue,
    ) -> None:
        tmo_instance: TMO = self.__tmo_id_and_instance[object_instance.tmo_id]

        tmo_attributes = {
            tmo_instance.latitude: "latitude",
            tmo_instance.longitude: "longitude",
            tmo_instance.status: "status",
        }

        attribute_name = tmo_attributes.get(tprm_instance.id)

        if attribute_name:
            setattr(object_instance, attribute_name, value_to_create.new_value)

        self.repository.add_object_to_session(object_instance=object_instance)

    def _update_formula_parameter(self, formula) -> None:
        prepared_param: PRM = formula["param_instance"]
        prepared_param.value = calculate_by_formula_new(
            session=self.session,
            param_type=formula["param_type"],
            object_instance=formula["db_mo"],
            x=formula["param_value"],
        )
        prepared_param.version = 1

        self.repository.add_prm_to_session(prm=prepared_param)
        self.__prepared_params_for_create.append(prepared_param)

    def check(self):
        self._get_mo_instances()
        self._get_tprm_instances()
        self._check_parameters_for_existence()

    def execute(self):
        objects_to_update = set()

        for object_inst in self.data_for_create:
            self._create_parameter(object_with_requested_parameters=object_inst)
            objects_to_update.add(object_inst.object_id)

        for formula in self.__formula_params:
            self._update_formula_parameter(formula=formula)
            objects_to_update.add(formula["db_mo"].id)

        for object_id in objects_to_update:
            db_mo = self.__mo_id_and_instance[object_id]
            update_object_version_and_modification_date(
                session=self.session, object_instance=db_mo
            )

        prepared_params_for_create = copy.deepcopy(
            self.__prepared_params_for_create
        )

        self.session.commit()
        return prepared_params_for_create


def delete_parameter_instance(
    tprm: TPRM, session: Session, param: PRM, object_id: int
):
    if tprm and tprm.val_type == two_way_mo_link_val_type_name:
        try:
            delete_two_way_mo_link_prms(
                session=session,
                prm_ids=[param.id],
                in_case_of_error=ErrorHandlingType.RAISE_ERROR,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
    else:
        if param.tprm.required:
            raise HTTPException(
                status_code=409,
                detail="Required parameter for object. Not allowed to delete.",
            )
        if param.tprm.val_type == "sequence":
            raise HTTPException(
                status_code=409, detail="Not allowed to delete sequence member!"
            )

        session.info["disable_security"] = True
        db_mo = session.get(MO, object_id)
        session.info["disable_security"] = True
        db_tmo = session.get(TMO, db_mo.tmo_id)

        for attribute in ["longitude", "latitude", "status"]:
            update_param_attributes(
                session=session,
                db_tmo=db_tmo,
                db_mo=db_mo,
                param_type_id=tprm.id,
                attribute=attribute,
            )
        delete_prm_links_by_prm_id(session=session, parameter_instance=param)
        session.delete(param)
        update_object_version_and_modification_date(
            session=session, object_instance=db_mo
        )


def get_parameters_of_object(
    session: Session, object_id: MO.id, only_returnable: bool = False
):
    if only_returnable:
        object_params = (
            session.execute(select(PRM).where(PRM.mo_id == object_id))
            .scalars()
            .all()
        )
    else:
        stmt = (
            select(PRM)
            .join(TPRM)
            .where(PRM.mo_id == object_id, TPRM.returnable.is_(True))
        )
        object_params = session.execute(stmt).scalars().all()
    return object_params


def get_links_of_parameters(
    parameters: list[PRM], val_type: Literal["mo_link", "prm_link"]
) -> dict[PRM.id, Union[list[PRM.value]]]:
    all_links = {}
    for parameter in parameters:
        if parameter.tprm.val_type == val_type:
            if parameter.tprm.multiple:
                all_links[parameter.id] = decode_multiple_value(parameter.value)
            else:
                all_links[parameter.id] = parameter.value

    return all_links


class ParameterDBGetter:
    def __init__(self, session: Session):
        self._session = session

    def _get_parameters_by_ids(
        self, parameter_ids: set[int] | list[int]
    ) -> dict[int, PRM]:
        parameter_instance_by_id = dict()

        for chunk in get_chunked_values_by_sqlalchemy_limit(parameter_ids):
            query = select(PRM).where(PRM.id.in_(chunk))
            temp_parameter_instance_by_id = {
                parameter_instance.id: parameter_instance
                for parameter_instance in self._session.execute(query)
                .scalars()
                .all()
            }
            parameter_instance_by_id.update(temp_parameter_instance_by_id)

        return parameter_instance_by_id
