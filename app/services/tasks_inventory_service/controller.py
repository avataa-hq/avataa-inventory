from fastapi import HTTPException
from sqlalchemy import select, cast, Integer
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from common.common_constant import NAME_DELIMITER
from functions.db_functions.db_create import create_db_object
from functions.db_functions.db_read import (
    get_db_object_type_or_exception,
    get_db_object_or_exception,
    get_db_param_type_or_exception_422,
)
from functions.functions_utils.utils import calculate_by_formula_new
from functions.validation_functions.validation_function import (
    check_if_all_required_params_passed,
)
from models import MO, TPRM, PRM
from routers.object_router.schemas import MOCreateWithParams
from routers.parameter_router.schemas import PRMCreateByMO
from routers.object_router.utils import (
    validate_object_parameters,
    proceed_parameter_attributes,
    update_geometry,
)
from val_types.constants import enum_val_type_name


class TasksInventoryController:
    def __init__(self, session: Session):
        self.session = session

    def create_object_with_params(self, obj: MOCreateWithParams) -> None:
        """
        Creates Object (MO) and its parameters (PRM)
        :param obj: object data and parameters
        :raises HTTPException: ...
        """
        check_if_all_required_params_passed(session=self.session, object=obj)
        db_object_type = get_db_object_type_or_exception(
            session=self.session, object_type_id=obj.tmo_id
        )
        db_object = create_db_object(
            session=self.session,
            object_to_update=obj,
            object_type=db_object_type,
        )
        if obj.p_id:
            parent = get_db_object_or_exception(
                session=self.session, object_id=obj.p_id
            )
        self.session.add(db_object)
        primary_values = []
        label_values = []

        sequence_params = self.session.execute(
            select(TPRM).where(
                TPRM.tmo_id == obj.tmo_id, TPRM.val_type == "sequence"
            )
        )
        sequence_params = sequence_params.scalars().all()
        for seq_param in sequence_params:
            for param in obj.params:
                if param.tprm_id == seq_param.id:
                    break
            else:
                obj.params.append(PRMCreateByMO(tprm_id=seq_param.id))

        for param in obj.params:
            validate_object_parameters(
                self.session, param, db_object, obj.params
            )

            self.session.info["disable_security"] = True
            latitude_id = db_object_type.latitude
            longitude_id = db_object_type.longitude
            status_id = db_object_type.status

        db_param_list = []
        for param in obj.params:
            db_param_type = get_db_param_type_or_exception_422(
                session=self.session, tprm_id=param.tprm_id
            )

            if db_param_type.val_type == enum_val_type_name:
                continue
            else:
                if db_param_type.val_type == "sequence":
                    query = select(PRM).where(
                        PRM.tprm_id == db_param_type.id,
                        cast(PRM.value, Integer) >= param.value,
                    )
                    if db_param_type.constraint:
                        sequence_type = [
                            p
                            for p in obj.params
                            if p.tprm_id == int(db_param_type.constraint)
                        ]
                        sequence_type = sequence_type[0].value
                        subquery = (
                            select(PRM.mo_id)
                            .where(
                                PRM.value == str(sequence_type),
                                PRM.tprm_id == int(db_param_type.constraint),
                            )
                            .scalar_subquery()
                        )
                        query = query.where(PRM.mo_id.in_(subquery))
                    query = query.execution_options(yield_per=100)
                    params_to_update = (
                        self.session.execute(query).scalars().partitions(100)
                    )
                    for chunk in params_to_update:
                        for param_to_update in chunk:
                            param_to_update.value = str(
                                int(param_to_update.value) + 1
                            )
                            self.session.add(param_to_update)

                db_param = PRM(
                    tprm_id=param.tprm_id, mo_id=db_object.id, value=param.value
                )
                self.session.add(db_param)
                try:
                    self.session.flush()
                    self.session.refresh(db_param)
                    self.session.flush()
                except IntegrityError:
                    continue
                self.session.refresh(db_param)

                if db_param.tprm_id == longitude_id:
                    db_object.longitude = float(db_param.value)
                if db_param.tprm_id == latitude_id:
                    db_object.latitude = float(db_param.value)
                if db_param.tprm_id == status_id:
                    db_object.status = db_param.value

                param_to_read = proceed_parameter_attributes(
                    parameter_instance=db_param
                )

                db_param_list.append(param_to_read)

        for tprm in db_object_type.tprms:
            if tprm.val_type == "formula":
                try:
                    value = calculate_by_formula_new(
                        session=self.session,
                        param_type=tprm,
                        object_instance=db_object,
                    )
                except ValueError as ex:
                    raise HTTPException(status_code=422, detail=f"{ex}")
                db_param = PRM(tprm_id=tprm.id, mo_id=db_object.id, value=value)
                self.session.add(db_param)
                try:
                    self.session.flush()
                    self.session.refresh(db_param)
                    self.session.flush()
                except IntegrityError as ex:
                    if ex.orig.pgcode == "23505":  # UniqueViolation
                        raise HTTPException(
                            status_code=409,
                            detail="You can't create formula TPRM. Unique violation.",
                        )
                    continue
                self.session.refresh(db_param)
        self.session.flush()
        if len(db_object_type.primary) > 0:
            for primary_tprm_id in db_object_type.primary:
                for param in db_object.prms:
                    if param.tprm_id == primary_tprm_id:
                        tprm_val_type = self.session.exec(
                            select(TPRM.val_type).where(
                                TPRM.id == param.tprm_id
                            )
                        ).first()

                        if tprm_val_type == "mo_link":
                            prm_value = self.session.exec(
                                select(MO.name).where(MO.id == int(param.value))
                            ).first()
                        else:
                            prm_value = str(param.value)

                        primary_values.append(prm_value)
                        break
            if not db_object_type.global_uniqueness and obj.p_id is not None:
                self.session.info["disable_security"] = True
                primary_values.insert(0, parent.name)
            name = NAME_DELIMITER.join(primary_values)
            self.session.info["disable_security"] = True
            name_exist = self.session.exec(
                select(MO).where(MO.name == name, MO.tmo_id == obj.tmo_id)
            ).first()
            if name_exist:
                raise HTTPException(
                    status_code=409,
                    detail=f"Object with name '{name}' already exists.",
                )
        self.session.flush()
        if len(primary_values) == 0:
            db_object.name = db_object.id
        else:
            db_object.name = name

        for label_tprm_id in db_object_type.label:
            for param in db_object.prms:
                if param.tprm_id == label_tprm_id:
                    tprm_val_type = self.session.exec(
                        select(TPRM.val_type).where(TPRM.id == param.tprm_id)
                    ).first()

                    if tprm_val_type == "mo_link":
                        prm_value = self.session.exec(
                            select(MO.name).where(MO.id == int(param.value))
                        ).first()
                    else:
                        prm_value = str(param.value)

                    label_values.append(prm_value)
                    break
        if label_values:
            label = NAME_DELIMITER.join(label_values)
            db_object.label = label
        if (
            db_object.point_a_id
            and db_object.point_b_id
            and db_object_type.geometry_type == "line"
            and not db_object.geometry
        ):
            point_a_whereclauses = [MO.id == db_object.point_a_id]
            point_b_whereclauses = [MO.id == db_object.point_b_id]
            if db_object_type.points_constraint_by_tmo:
                point_a_whereclauses.append(
                    MO.tmo_id.in_(db_object_type.points_constraint_by_tmo)
                )
                point_b_whereclauses.append(
                    MO.tmo_id.in_(db_object_type.points_constraint_by_tmo)
                )

            point_a = self.session.execute(
                select(MO).where(*point_a_whereclauses)
            ).scalar_one_or_none()
            point_b = self.session.execute(
                select(MO).where(*point_b_whereclauses)
            ).scalar_one_or_none()

            if not point_a:
                raise HTTPException(
                    status_code=422,
                    detail=f"You try to add point_a to MO with id {db_object.point_a_id}, "
                    f"which is not match with object constraint",
                )
            if not point_b:
                raise HTTPException(
                    status_code=422,
                    detail=f"You try to add point_b MO with id {db_object.point_b_id}, "
                    f"which is not match with object constraint",
                )
            db_object.geometry = update_geometry(
                object_instance=db_object, point_a=point_a, point_b=point_b
            )
        # Set parent coords data
        if db_object_type.inherit_location and obj.p_id:
            if parent.latitude:
                db_object.latitude = parent.latitude
            if parent.longitude:
                db_object.longitude = parent.longitude
            if parent.geometry:
                db_object.geometry = parent.geometry
            if parent.point_a_id:
                db_object.point_a_id = parent.point_a_id
            if parent.point_b_id:
                db_object.point_b_id = parent.point_b_id
        self.session.add(db_object)
        self.session.flush()
        self.session.commit()
        self.session.refresh(db_object)
