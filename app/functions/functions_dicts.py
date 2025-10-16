import ast
import re
from decimal import Decimal

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlmodel import select

from functions.formula_parser import evaluate_formula
from models import TPRM, TMO
from routers.parameter_router.schemas import (
    PRMReadFloat,
    PRMReadInt,
    PRMReadStr,
    PRMReadBool,
)
from routers.parameter_type_router.schemas import TPRMCreate, TPRMUpdate
from val_types.constants import (
    enum_val_type_name,
    two_way_mo_link_val_type_name,
)

db_param_convert_by_val_type = {
    "str": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadStr(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "date": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadStr(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "datetime": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadStr(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "float": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadFloat(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "int": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadInt(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "mo_link": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadInt(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "prm_link": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadInt(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "user_link": lambda id,
    tprm_id,
    mo_id,
    value,
    version,
    **kwargs: PRMReadStr(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "formula": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadStr(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "bool": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadBool(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    "sequence": lambda id, tprm_id, mo_id, value, version, **kwargs: PRMReadInt(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    two_way_mo_link_val_type_name: lambda id,
    tprm_id,
    mo_id,
    value,
    version,
    **kwargs: PRMReadInt(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
    enum_val_type_name: lambda id,
    tprm_id,
    mo_id,
    value,
    version,
    **kwargs: PRMReadStr(
        id=id, tprm_id=tprm_id, mo_id=mo_id, value=value, version=version
    ),
}


def str_constraint_validation(param_type, session):
    try:
        re.compile(param_type.constraint)
    except re.error:
        raise HTTPException(
            status_code=422, detail="Invalid regex for string parameter type."
        )


def int_constraint_validation(param_type, session):
    min_value, max_value = param_type.constraint.split(":")

    try:
        if min_value and max_value:
            if int(max_value) < int(min_value):
                raise HTTPException(
                    status_code=422,
                    detail="Invalid constraint parameter for int parameter type:"
                    "max value greater than min.",
                )
            if int(max_value) == int(min_value):
                raise HTTPException(
                    status_code=422,
                    detail="Invalid constraint parameter for int parameter type:"
                    "max can't be equal to min.",
                )
        else:
            raise HTTPException(
                status_code=422,
                detail="Invalid constraint parameter for int parameter type:"
                "add min and max values",
            )
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid constraint parameter for int parameter type {min_value, max_value}.",
        )


def float_constraint_validation(param_type, session):
    try:
        min_val, max_val = param_type.constraint.split(":")
        min_value = float(min_val)
        max_value = float(max_val)
        if min_value and max_value:
            if max_value <= min_value:
                raise HTTPException(
                    status_code=422,
                    detail="Invalid constraint parameter for float parameter type:"
                    " max value greater than min.",
                )

        else:
            raise HTTPException(
                status_code=422, detail="Add min and max value for constraint"
            )
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=422,
            detail="Invalid constraint parameter for float parameter type.",
        )


def prm_link_constraint_validation(param_type, session):
    try:
        int(param_type.constraint)
    except BaseException:
        raise HTTPException(
            status_code=422,
            detail="Invalid constraint parameter for prm_link: id expected.",
        )
    linked_tprm = session.get(TPRM, param_type.constraint)
    if not linked_tprm:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid constraint parameter for prm_link:"
            f" parameter type with id {param_type.constraint} not found.",
        )
    if param_type.prm_link_filter is not None:
        regex = re.compile(r"(\d+):(\d+)")
        if regex.fullmatch(param_type.prm_link_filter):
            internal_id, external_id = regex.findall(
                param_type.prm_link_filter
            )[0]

            internal_tprm = session.exec(
                select(TPRM).where(
                    TPRM.id == internal_id,
                    TPRM.val_type == "prm_link",
                    TPRM.tmo_id == param_type.tmo_id,
                )
            ).first()

            if not internal_tprm:
                raise HTTPException(
                    status_code=422,
                    detail=f"Internal TPRM  with id {internal_id} wasn't found in TMO with "
                    f"id {param_type.tmo_id}",
                )

            external_tprm = session.exec(
                select(TPRM).where(
                    TPRM.id == external_id,
                    TPRM.id != linked_tprm.id,
                    TPRM.val_type == "prm_link",
                    TPRM.tmo_id == linked_tprm.tmo_id,
                )
            ).first()

            if not external_tprm:
                raise HTTPException(
                    status_code=422,
                    detail=f"External TPRM with id {external_id} wasn't found in TMO with "
                    f"id {linked_tprm.tmo_id}",
                )

            if int(internal_tprm.constraint) == int(external_tprm.constraint):
                return True
            else:
                raise HTTPException(
                    status_code=422,
                    detail="Internal TPRM and external TPRM have to refer to the same TPRM. "
                    "Or internal TPRM can be referred to external TPRM",
                )

        else:
            raise HTTPException(
                status_code=422, detail="Invalid prm_link_filter."
            )


def mo_link_constraint_validation(param_type, session):
    try:
        int(param_type.constraint)
    except BaseException:
        raise HTTPException(
            status_code=422,
            detail="Invalid constraint parameter for mo_link: id expected.",
        )
    if not session.get(TMO, param_type.constraint):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid constraint parameter for mo_link:"
            f" object type with id {param_type.constraint} not found.",
        )


def formula_constraint_validation_new(param_type, session):
    nodes = {}
    try:
        if ";" in param_type.constraint:
            str_pattern = re.compile(r"'.+'")
            conditions: list[str] = param_type.constraint.split(";")
            if_state = conditions[0]
            regex = re.compile(
                r"(?:if|elif) (.*) (==|!=|>|>=|<=|<) (?:and|or (==|!=|>|>=|<=|<))?(.*)? then (.*)"
            )
            if regex.fullmatch(if_state):
                expressions = regex.findall(if_state)[0]
                if not str_pattern.fullmatch(expressions[0]):
                    nodes[expressions[0]] = ast.parse(
                        expressions[0], "<string>", mode="eval"
                    )
            else:
                raise HTTPException(
                    status_code=422, detail="Could not parse formula"
                )
            else_state = conditions[-1]
            regex = re.compile(r" else (.+)")
            if else_state:
                match_result = regex.fullmatch(else_state)
                if match_result:
                    expressions = regex.findall(else_state)
                    first_expression = expressions[0]
                    if not str_pattern.fullmatch(first_expression):
                        nodes[first_expression] = ast.parse(
                            first_expression, "<string>", mode="eval"
                        )
                else:
                    raise HTTPException(
                        status_code=422, detail="Could not parse formula"
                    )
            for i in range(1, len(conditions) - 1):
                regex = re.compile(
                    r" (?:if|elif) (.*) (==|!=|>|>=|<=|<) (?:and|or (==|!=|>|>=|<=|<))?(.*)? then (.*)"
                )
                if regex.fullmatch(conditions[i]):
                    expressions = regex.findall(conditions[i])[0]
                    if not str_pattern.fullmatch(expressions[0]):
                        nodes[expressions[0]] = ast.parse(
                            expressions[0], "<string>", mode="eval"
                        )
                else:
                    raise HTTPException(
                        status_code=422, detail="Could not parse formula"
                    )
        else:
            nodes[param_type.constraint] = ast.parse(
                param_type.constraint, "<string>", mode="eval"
            )
    except SyntaxError:
        raise HTTPException(status_code=422, detail="Could not parse formula")
    for formula, node in nodes.items():
        names = sorted(
            {nd.id for nd in ast.walk(node) if isinstance(nd, ast.Name)}
        )
        values = {}
        for name in names:
            values[name] = 1
        evaluate_formula(formula, values)


def non_constraint_validation(param_type, session):
    pass


def sequence_constraint_validation(
    param_type: TPRMCreate | TPRMUpdate, session: Session
):
    try:
        int(param_type.constraint)
    except BaseException:
        raise HTTPException(
            status_code=422,
            detail="Invalid constraint parameter for sequence: id expected.",
        )

    query = select(TPRM).where(
        TPRM.id == param_type.constraint, TPRM.tmo_id == param_type.tmo_id
    )
    linked_tprm = session.execute(query)
    linked_tprm = linked_tprm.scalar()
    if not linked_tprm:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid constraint parameter for sequence:"
            f" parameter type with id {param_type.constraint} not found in this object type!.",
        )
    if not linked_tprm.required:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid constraint parameter for sequence:"
            f" parameter type with id {param_type.constraint} must be required!.",
        )

    if linked_tprm.multiple:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid constraint parameter for sequence:"
            f" parameter type with id {param_type.constraint} must not be multiple!.",
        )


param_type_constraint_validation = {
    "str": str_constraint_validation,
    "date": non_constraint_validation,
    "datetime": non_constraint_validation,
    "float": float_constraint_validation,
    "int": int_constraint_validation,
    "bool": non_constraint_validation,
    "prm_link": prm_link_constraint_validation,
    "mo_link": mo_link_constraint_validation,
    "user_link": str_constraint_validation,
    "formula": formula_constraint_validation_new,
    "sequence": sequence_constraint_validation,
    two_way_mo_link_val_type_name: mo_link_constraint_validation,
}


def str_constraint_error_validation(param_type, tmo_id, error_list, session):
    try:
        re.compile(param_type.constraint)
        return True
    except re.error:
        error_list.append({"error": "Invalid regex for string parameter type."})
        return False


def int_constraint_error_validation(param_type, tmo_id, error_list, session):
    try:
        min_val, max_val = param_type.constraint.split(":")
        min_value = int(min_val)
        max_value = int(max_val)

        if min_val.isdigit() and max_val.isdigit():
            if max_value <= min_value:
                error_list.append(
                    {
                        "error": f"Invalid constraint parameter for {param_type.val_type} parameter type:"
                        f" max value greater than min."
                    }
                )
                return False

        else:
            raise ValueError
    except ValueError:
        error_list.append(
            {
                "error": f"Invalid constraint parameter for {param_type.val_type} parameter type."
            }
        )
        return False
    else:
        return True


def float_constraint_error_validation(param_type, tmo_id, error_list, session):
    try:
        min_val, max_val = param_type.constraint.split(":")

        min_value = float(min_val)
        max_value = float(max_val)

        if min_value and max_value:
            if max_value <= min_value:
                error_list.append(
                    {
                        "error": f"Invalid constraint parameter for {param_type.val_type} parameter type:"
                        f" max value greater than min."
                    }
                )
                return False
    except ValueError:
        error_list.append(
            {
                "error": f"Invalid constraint parameter for {param_type.val_type} parameter type."
            }
        )
        return False
    else:
        return True


def mo_link_constraint_error_validation(
    param_type, tmo_id, error_list, session
):
    try:
        int(param_type.constraint)
    except BaseException:
        error_list.append(
            {"error": "Invalid constraint parameter for mo_link: id expected."}
        )
        return False
    if not session.get(TMO, param_type.constraint):
        error_list.append(
            {
                "error": f"Invalid constraint parameter for mo_link:"
                f" object type with id {param_type.constraint} not found."
            }
        )
        return False
    return True


def prm_link_constraint_error_validation(
    param_type, tmo_id, error_list, session
):
    try:
        int(param_type.constraint)
    except BaseException:
        error_list.append(
            {"error": "Invalid constraint parameter for prm_link: id expected."}
        )
        return False
    linked_tprm = session.exec(
        select(TPRM).where(TPRM.id == param_type.constraint)
    ).first()
    if not linked_tprm:
        error_list.append(
            {
                "error": f"Invalid constraint parameter for prm_link:"
                f" parameter type with id {param_type.constraint} not found."
            }
        )
        return False
    if param_type.prm_link_filter is not None:
        regex = re.compile(r"(\d+):(\d+)")
        if regex.fullmatch(param_type.prm_link_filter):
            internal_id, external_id = regex.findall(
                param_type.prm_link_filter
            )[0]

            internal_tprm = session.exec(
                select(TPRM).where(
                    TPRM.id == internal_id,
                    TPRM.val_type == "prm_link",
                    TPRM.tmo_id == tmo_id,
                )
            ).first()

            if not internal_tprm:
                error_list.append(
                    {
                        "error": f"Internal TPRM  with id {internal_id} wasn't found in TMO with id {tmo_id}"
                    }
                )
                return False

            external_tprm = session.exec(
                select(TPRM).where(
                    TPRM.id == external_id,
                    TPRM.id != linked_tprm.id,
                    TPRM.val_type == "prm_link",
                    TPRM.tmo_id == linked_tprm.tmo_id,
                )
            ).first()

            if not external_tprm:
                error_list.append(
                    {
                        "error": f"External TPRM with id {external_id} wasn't "
                        f"found in TMO with id {linked_tprm.object_type_id}"
                    }
                )
                return False

            if int(internal_tprm.constraint) == int(external_tprm.constraint):
                return True
            else:
                error_list.append(
                    {
                        "error": "Internal TPRM and external TPRM have to refer to the same TPRM. "
                        "Or internal TPRM can be referred to external TPRM"
                    }
                )
                return False

        else:
            error_list.append({"error": "Invalid prm_link_filter."})
            return False
    return True


def non_constraint_error_validation(param_type, tmo_id, error_list, session):
    return True


def formula_constraint_error_validation(
    param_type, tmo_id, error_list, session
):
    nodes = {}
    try:
        if ";" in param_type.constraint:
            str_pattern = re.compile(r"'.+'")
            conditions = param_type.constraint.split(";")
            if_state = conditions[0]
            regex = re.compile(
                r"if ([0-9a-zA-Z.']+ (?:==|!=|>|>=|<=|<) [0-9a-zA-Z.']+(?: (?:and|or) [0-9a-zA-Z.']+ "
                r"(?:==|!=|>|>=|<=|<) [0-9a-zA-Z.']+)*) then (.+)"
            )
            if regex.fullmatch(if_state):
                expressions = regex.findall(if_state)[0]
                if not str_pattern.fullmatch(expressions[1]):
                    nodes[expressions[1]] = ast.parse(
                        expressions[1], "<string>", mode="eval"
                    )
            else:
                error_list.append({"error": "Could not parse formula"})
                return False
            else_state = conditions[-1]
            regex = re.compile(r" else (.+)")
            if regex.fullmatch(else_state):
                expressions = regex.findall(else_state)
                if not str_pattern.fullmatch(expressions[0]):
                    nodes[expressions[0]] = ast.parse(
                        expressions[0], "<string>", mode="eval"
                    )
            else:
                error_list.append({"error": "Could not parse formula"})
                return False
            for i in range(1, len(conditions) - 1):
                regex = re.compile(
                    r" elif ([0-9a-zA-Z.']+ (?:==|!=|>|>=|<=|<) [0-9a-zA-Z.']+(?: (?:and|or) [0-9a-zA-Z.']+ "
                    r"(?:==|!=|>|>=|<=|<) [0-9a-zA-Z.']+)*) then (.+)"
                )
                if regex.fullmatch(conditions[i]):
                    expressions = regex.findall(conditions[i])[0]
                    if not str_pattern.fullmatch(expressions[1]):
                        nodes[expressions[1]] = ast.parse(
                            expressions[1], "<string>", mode="eval"
                        )
                else:
                    error_list.append({"error": "Could not parse formula"})
                    return False
        else:
            nodes[param_type.constraint] = ast.parse(
                param_type.constraint, "<string>", mode="eval"
            )
    except SyntaxError:
        error_list.append({"error": "Could not parse formula"})
        return False
    for formula, node in nodes.items():
        names = sorted(
            {nd.id for nd in ast.walk(node) if isinstance(nd, ast.Name)}
        )
        values = {}
        for name in names:
            values[name] = 1
        try:
            evaluate_formula(formula, values)
        except HTTPException as e:
            error_list.append({"error": e.detail})
            return False
        except Exception:
            error_list.append({"error": "Formula evaluation failed"})
            return False
    return True


def sequence_constraint_error_validation(
    param_type: TPRMCreate, tmo_id: int, error_list: list, session: Session
):
    if not param_type.constraint.isdigit():
        query = select(TPRM.id).where(
            TPRM.tmo_id == tmo_id, TPRM.name == param_type.constraint
        )
        constraint_real_id = session.execute(query)
        constraint_real_id = constraint_real_id.scalar()
        if not constraint_real_id:
            error_list.append(
                {
                    "error": "Invalid constraint parameter for sequence: id expected."
                }
            )
            return False
        param_type.constraint = constraint_real_id

    query = select(TPRM).where(
        TPRM.id == int(param_type.constraint), TPRM.tmo_id == tmo_id
    )
    linked_tprm = session.execute(query)
    linked_tprm = linked_tprm.scalar()
    if not linked_tprm:
        error_list.append(
            {
                "error": f"Invalid constraint parameter for sequence: "
                f"parameter type with id {param_type.constraint} not found in this object type!"
            }
        )
        return False

    return True


error_param_type_constraint_validation = {
    "str": str_constraint_error_validation,
    "date": non_constraint_error_validation,
    "datetime": non_constraint_error_validation,
    "float": float_constraint_error_validation,
    "int": int_constraint_error_validation,
    "bool": non_constraint_error_validation,
    "mo_link": mo_link_constraint_error_validation,
    "prm_link": prm_link_constraint_error_validation,
    "user_link": str_constraint_error_validation,
    "formula": formula_constraint_error_validation,
    "sequence": sequence_constraint_error_validation,
}


def str_convertation(value):
    return value


def bool_convertation(value):
    if value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False


def float_convertation(value):
    return Decimal(value)


def int_convertation(value):
    return int(value)


def link_convertation(value):
    if isinstance(value, int):
        return value

    if value.isdigit():
        return int(value)

    return value


value_convertation_by_val_type = {
    "str": str_convertation,
    "date": str_convertation,
    "datetime": str_convertation,
    "float": float_convertation,
    "int": int_convertation,
    "bool": bool_convertation,
    "mo_link": link_convertation,
    "prm_link": link_convertation,
    "user_link": str_convertation,
    "formula": str_convertation,
    "sequence": int_convertation,
    two_way_mo_link_val_type_name: int_convertation,
}


def extract_formula_parameters(formula: str) -> list[str] | None:
    pattern = r"parameter\['([^']+)'\]"
    tprm_names = re.findall(pattern, formula)
    if tprm_names:
        result = []
        for tprm_name in set(tprm_names):
            result.append(tprm_name)
        return result
