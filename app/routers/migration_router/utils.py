import ast

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlmodel import select

from functions.functions_dicts import formula_constraint_validation_new
from models import TMO, TPRM
from routers.migration_router.constants import (
    SEPARATOR_FOR_PRM_LINK,
    CONVERT_TO_STRING_SYMBOL,
)
from routers.migration_router.exceptions import NotValidValue
from routers.object_type_router.schemas_validator import (
    lifecycle_process_definition_validator,
)
from services.security_service.routers.utils.recursion import (
    get_items_recursive_up,
)
from val_types.constants import two_way_mo_link_val_type_name


def get_all_object_type_ids(data: dict) -> list:
    result = []

    if "object_type_id" in data:
        result.append(data["object_type_id"])

    if "children" in data:
        for child in data["children"]:
            result.extend(get_all_object_type_ids(child))

    return result


def build_tree(nodes: list[dict], parent_id: int = None) -> list[dict]:
    tree = []
    for node in nodes:
        if node["parent_id"] == parent_id:
            children = build_tree(nodes, node["object_type_id"])
            node["children"] = children
            tree.append(node)

    return tree


def get_all_child_object_type_ids(
    session: Session, main_object_type_instance: TMO
):
    query = """
    WITH RECURSIVE descendants AS (
        SELECT
            tmo.id AS object_type_id,
            tmo.p_id AS parent_id
        FROM TMO
        WHERE id = :start_object_type_id

        UNION ALL

        SELECT
            tmo.id AS object_type_id,
            tmo.p_id AS parent_id
        FROM TMO tmo
        INNER JOIN descendants d ON tmo.p_id = d.object_type_id
    )
    SELECT * FROM descendants;
    """

    params_for_query = {"start_object_type_id": main_object_type_instance.id}

    nodes = [
        dict(row)
        for row in session.execute(
            statement=text(query), params=params_for_query
        ).mappings()
    ]

    tree = build_tree(nodes=nodes, parent_id=main_object_type_instance.p_id)

    if tree:
        return get_all_object_type_ids(data=tree[0])

    return []


def get_all_parent_object_type_ids(
    session: Session, main_object_type_instance: TMO
):
    object_instances_ids = get_items_recursive_up(
        session=session,
        main_table=TMO,
        instance_id=main_object_type_instance.id,
    )

    if object_instances_ids:
        return sorted(object_instances_ids)

    return []


class MigrateObjectTypeUtils:
    def __init__(self, session: Session):
        self._session = session

    def process_parameter_type_for_object_type(
        self, value: str, object_type_instance: TMO
    ):
        if not value:
            return None

        query = select(TPRM).where(
            TPRM.tmo_id == object_type_instance.id,
            TPRM.name == value,
        )
        parameter_type_instance = self._session.execute(query).scalar()
        if parameter_type_instance:
            return parameter_type_instance.id

        raise NotValidValue(
            status_code=422,
            detail=f"Parameter type with name {value} is not exists for object type {object_type_instance.name}",
        )

    def process_list_parameter_types_for_object_type(
        self, value: str, object_type_instance: TMO
    ):
        if not value:
            return []

        value = ast.literal_eval(value)

        query = select(TPRM).where(
            TPRM.tmo_id == object_type_instance.id,
            TPRM.name.in_(value),
        )
        parameter_type_id_by_name = {
            parameter_type_instance.name: parameter_type_instance.id
            for parameter_type_instance in self._session.execute(query)
            .scalars()
            .all()
        }

        new_value = []
        for parameter_type_name in value:
            parameter_type_id = parameter_type_id_by_name.get(
                parameter_type_name
            )
            if parameter_type_id:
                new_value.append(parameter_type_id)
                continue

            raise NotValidValue(
                status_code=422,
                detail=f"Parameter type with name {value} is not exists for object type {object_type_instance.name}",
            )
        return new_value

    def convert_object_type_parent_id(
        self, value: str, object_type_instance: TMO
    ):
        if not value:
            return None

        query = select(TMO).where(TMO.name == value)
        exists_object_type = self._session.execute(query).scalar()
        if exists_object_type:
            return exists_object_type.id

        raise NotValidValue(
            status_code=422,
            detail=f"Requested object type name as parent {value} does not exists",
        )

    def process_prm_link_filter_value(
        self, value: str, parameter_type_instance: TPRM
    ):
        if not value:
            return None

        if value[0] != CONVERT_TO_STRING_SYMBOL:
            raise NotValidValue(
                status_code=422,
                detail=f"Parameter link has to has {CONVERT_TO_STRING_SYMBOL} symbol in first place",
            )

        try:
            parameter_type_name, object_type_name = (
                parameter_type_instance.constraint.split(SEPARATOR_FOR_PRM_LINK)
            )
        except Exception as e:
            print(e)
            raise NotValidValue(
                status_code=422,
                detail="Constraint for value type 'parameter link' has to has specific form: "
                "'PARAMETER TYPE NAME::OBJECT TYPE NAME'",
            )

        query = (
            select(TPRM)
            .join(TMO)
            .where(
                TPRM.name == parameter_type_name, TMO.name == object_type_name
            )
        )
        linked_parameter_type = self._session.execute(query).scalar()

        if not linked_parameter_type:
            raise NotValidValue(
                status_code=422,
                detail=f"Requested parameter type name {parameter_type_name} as constraint "
                f"for {parameter_type_instance.name} does not exists in {object_type_name}",
            )

        left_part, right_part = value.split(":")
        left_part = left_part[1:]

        left_part = self._session.execute(
            select(TPRM).where(
                TPRM.name == left_part,
                TPRM.val_type == "prm_link",
                TPRM.tmo_id == parameter_type_instance.tmo_id,
            )
        ).scalar()

        if not left_part:
            raise NotValidValue(
                status_code=422,
                detail=f"Internal parameter type with name {left_part} wasn't found in requested object type",
            )

        right_part = self._session.exec(
            select(TPRM).where(
                TPRM.name == right_part,
                TPRM.name != left_part.name,
                TPRM.val_type == "prm_link",
                TPRM.tmo_id == linked_parameter_type.tmo_id,
            )
        ).first()

        if not right_part:
            raise NotValidValue(
                status_code=422,
                detail=f"External parameter type with name {right_part} wasn't found in requested object type",
            )

        if int(left_part.constraint) == int(right_part.constraint):
            return f"{left_part.id}:{right_part.id}"

        else:
            raise NotValidValue(
                status_code=422,
                detail="Internal parameter type and external parameter type have to refer to the same parameter type. "
                "Or internal parameter type can be referred to external parameter type",
            )

    def process_backward_link_value(
        self, value: str, parameter_type_instance: TPRM
    ):
        if not value:
            return None

        query = select(TPRM).where(
            TPRM.tmo_id == parameter_type_instance.tmo_id, TPRM.name == value
        )
        backward_link = self._session.execute(query).scalar()
        if backward_link:
            return backward_link.id

        raise NotValidValue(
            status_code=422,
            detail=f"Requested parameter type name {value} does not exists",
        )

    def process_constraint_value(
        self, value: str, parameter_type_instance: TPRM
    ):
        if not value:
            return None

        if parameter_type_instance.val_type in [
            "mo_link",
            two_way_mo_link_val_type_name,
        ]:
            query = select(TMO.id).where(TMO.name == value)
            object_type_id = self._session.execute(query).scalar()
            if object_type_id:
                return object_type_id

            raise NotValidValue(
                status_code=422,
                detail=f"Requested object type name {value} as constraint for "
                f"{parameter_type_instance.name} does not exists",
            )

        elif parameter_type_instance.val_type == "prm_link":
            try:
                parameter_type_name, object_type_name = value.split(
                    SEPARATOR_FOR_PRM_LINK
                )
            except Exception as e:
                print(e)
                raise NotValidValue(
                    status_code=422,
                    detail="Constraint for value type 'parameter link' has to has specific form: "
                    "'PARAMETER TYPE NAME::OBJECT TYPE NAME'",
                )

            query = (
                select(TPRM)
                .join(TMO)
                .where(
                    TPRM.name == parameter_type_name,
                    TMO.name == object_type_name,
                )
            )
            linked_parameter_type_instance = self._session.execute(
                query
            ).scalar()

            if linked_parameter_type_instance:
                return linked_parameter_type_instance.id

            raise NotValidValue(
                status_code=422,
                detail=f"Requested parameter type name {parameter_type_name} as constraint "
                f"for {parameter_type_instance.name} does not exists in {object_type_name}",
            )

        elif parameter_type_instance.val_type == "formula":
            formula_constraint_validation_new(
                param_type=parameter_type_instance, session=self._session
            )
            return value

        return value

    @staticmethod
    def convert_to_string(value: str):
        return value if value else None

    @staticmethod
    def convert_to_bool(value: str = ""):
        if not value:
            return None

        match value.lower():
            case "true" | "1":
                return True

            case "false" | "0":
                return False

            case _:
                print(value)
                raise NotValidValue(
                    status_code=422,
                    detail=f"Value {value} is not match with requested type bool. "
                    f'Allowed values: "True", "False"',
                )

    @staticmethod
    def check_lifecycle_process_definition_exists(
        value: str, object_type_instance: TMO
    ):
        try:
            lifecycle_process_definition_validator(value=value)
        except ValueError as e:
            raise NotValidValue(
                status_code=422,
                detail=str(e),
            )
        else:
            return value
