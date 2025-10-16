import ast
import pickle
import re
from datetime import datetime, timedelta
from typing import Any

import math
import sqlalchemy
from fastapi import HTTPException
from pandas import Series
from sqlalchemy import func
from sqlmodel import Session, select

from common.common_constant import NAME_DELIMITER
from functions.formula_parser import evaluate_formula
from models import MO, TPRM, PRM, TMO, GeometryType
from routers.parameter_type_router.schemas import TPRMUpdate


def count_objects(
    session: Session,
    object_type_id: int = None,
    mos_ids: list = None,
    p_id: int = None,
    active: bool = True,
) -> int:
    count_query = (
        select(func.count())
        .select_from(MO)
        .where(
            MO.id.in_(mos_ids) if mos_ids is not None else True,
            MO.tmo_id == object_type_id if object_type_id is not None else True,
            MO.p_id == p_id if p_id is not None else True,
            MO.active == active,
        )
    )
    quantity = session.exec(count_query).first()
    return quantity


def session_commit_create_or_exception(session: Session, message: str) -> None:
    try:
        session.commit()
    except sqlalchemy.exc.IntegrityError:
        raise HTTPException(status_code=409, detail=message)


def field_value_if_update_to_required(
    db_param_type: TPRM, param_type: TPRMUpdate, param_type_data: dict
) -> Any:
    print("in_field")
    print(param_type_data)
    if "field_value" in param_type_data:
        field_value = param_type_data.pop("field_value")

    elif not db_param_type.required and param_type.required:
        raise HTTPException(
            status_code=400,
            detail="Please, pass the 'field_value' when changing parameter type to required.",
        )
    else:
        field_value = None
    return field_value


def rename_linked_objects(session: Session, mo: MO):
    # get mo_link parameters that depends on given mo
    tprm_subquery = (
        select(TPRM.id).where(TPRM.val_type == "mo_link").scalar_subquery()
    )
    query = select(PRM).where(
        PRM.tprm_id.in_(tprm_subquery), PRM.value == str(mo.id)
    )
    params = session.exec(query).all()

    for param in params:
        # get corresponding tmo
        suqbuery = (
            select(TPRM.tmo_id)
            .where(TPRM.id == param.tprm_id)
            .scalar_subquery()
        )
        query = select(TMO).where(TMO.id == suqbuery)
        tmo = session.execute(query).scalar()

        # if tprm in primary -> rename object
        query = select(MO).where(MO.id == param.mo_id)
        linked_mo = None
        if tmo and param.tprm_id in tmo.primary:
            linked_mo = session.execute(query).scalar()
            rename_object_when_update_primary_prm(
                session=session, db_object=linked_mo, db_object_type=tmo
            )

        if tmo and param.tprm_id in tmo.label:
            if not linked_mo:
                linked_mo = session.execute(query).scalar()
            update_mo_label_when_update_label_prm(
                session=session, db_object=linked_mo, db_object_type=tmo
            )


def rename_object_when_update_primary_prm(
    session: Session, db_object: MO, db_object_type: TMO
) -> None:
    primary_values = dict()
    if not db_object_type.global_uniqueness and db_object.p_id is not None:
        session.info["disable_security"] = True
        parent = session.get(MO, db_object.p_id)
        primary_values["parent"] = parent.name

    primary_values.update(dict.fromkeys(db_object_type.primary))
    for tprm_id in primary_values.keys():
        for prm in db_object.prms:
            if prm.tprm_id == tprm_id:
                primary_values[tprm_id] = str(prm.value)

    for tprm_id, value in primary_values.items():
        if tprm_id == "parent":
            continue
        query = select(TPRM).where(
            TPRM.id == tprm_id, TPRM.val_type == "mo_link"
        )
        res = session.execute(query).scalar()
        if res:
            query = select(MO.name).where(MO.id == int(value))
            res = session.execute(query).scalar()
            primary_values[tprm_id] = res
    name = NAME_DELIMITER.join(primary_values.values())
    session.info["disable_security"] = True
    name_exist = session.exec(
        select(MO).where(
            MO.id != db_object.id,
            MO.name == name,
            MO.tmo_id == db_object_type.id,
        )
    ).first()
    if name_exist:
        raise HTTPException(
            status_code=422,
            detail="Unable to set this primary value. Object names will be not unique.",
        )
    db_object.name = name
    session.add(db_object)
    rename_linked_objects(session=session, mo=db_object)


def update_mo_label_when_update_label_prm(
    session: Session, db_object: MO, db_object_type: TMO
) -> None:
    label_values = {}

    label_values.update(dict.fromkeys(db_object_type.label))
    for tprm_id in label_values.keys():
        for prm in db_object.prms:
            if prm.tprm_id == tprm_id:
                label_values[tprm_id] = str(prm.value)
    query = select(TPRM.id).where(
        TPRM.id.in_(list(label_values.keys())), TPRM.val_type == "mo_link"
    )
    tprm_ids = session.execute(query).scalars()

    for tprm_id, value in label_values.items():
        if tprm_id in tprm_ids:
            query = select(MO.name).where(MO.id == int(value))
            res = session.execute(query).scalar()
            label_values[tprm_id] = res
    label = NAME_DELIMITER.join(label_values.values())

    db_object.label = label
    session.add(db_object)
    rename_linked_objects(session=session, mo=db_object)


def replace_constraint_prm_link(session: Session, param_types: TPRM) -> None:
    """
    dirty hack requested by frontender
    """

    def hack(tprm: TPRM):
        if tprm.val_type == "prm_link":
            constraint_tprm = session.get(TPRM, int(tprm.constraint))
            tprm.constraint = f"{constraint_tprm.tmo_id}:{tprm.constraint}"

    if isinstance(param_types, TPRM):
        hack(param_types)
    else:
        for param_type in list(param_types):
            hack(param_type)


def calculate_by_formula(
    session: Session, param_type: TPRM, mo_id: int, x: Any = None
) -> float:
    values = {}
    parameter = {}
    if x is not None:
        values["x"] = float(x)

    if ";" in param_type.constraint:
        conditions = param_type.constraint.split(";")
        for i in range(0, len(conditions) - 1):
            regex = re.compile(
                r"(?:if|elif) (parameter\['[^']+'] (?:==|!=|>|>=|<=|<) [0-9a-zA-Z.']+(?: (?:and|or) "
                r"parameter\['[^']+'] (?:==|!=|>|>=|<=|<) [0-9a-zA-Z.']+)*) then (.+)"
            )
            con = conditions[i]
            if_state = regex.findall(con)
            if if_state:
                if_state = if_state[0][0]
                pattern = r"parameter\['([^']+)'\]"

                tprm_names = re.findall(pattern, if_state)
                for tprm_name in set(tprm_names):
                    parameter[tprm_name] = tprm_name

                node = ast.parse(if_state, mode="eval")
                # Get list of all names in formula
                names = sorted(
                    {
                        nd.slice.value
                        for nd in ast.walk(node)
                        if isinstance(nd, ast.Subscript)
                    }
                )
                temp_values, temp_params = evaluate_prm_value(
                    session=session, names=names, mo_id=mo_id
                )
                values = values | temp_values
                parameter = parameter | temp_params
                try:
                    combined_values = {**values, "parameter": parameter}
                    if "datetime" in if_state:
                        pass
                    else:
                        is_true = eval(if_state, combined_values)
                except TypeError as e:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Incorrect comparison in statement: {e}.",
                    )
                # If main formula 'if expression' is not ok - we check 'else expression'
                if is_true:
                    formula = regex.findall(conditions[i])[0][1]
                    pattern = r"parameter\['([^']+)'\]"
                    tprm_names = re.findall(pattern, formula)
                    if tprm_names:
                        formula = re.sub(
                            pattern, str(values.get(*tprm_names)), formula
                        )
                    break
                else:
                    formula = None
            else:
                raise HTTPException(
                    status_code=422, detail="Could not parse formula"
                )

            if formula is None:
                # if we have 'else expression'
                else_state = conditions[-1]
                regex = re.compile(r" else (.+)")
                if else_state:
                    try:
                        formula = regex.findall(else_state)[0]
                        pattern = r"parameter\['([^']+)'\]"
                        tprm_names = re.findall(pattern, formula)
                        if tprm_names:
                            formula = re.sub(
                                pattern, str(values.get(*tprm_names)), formula
                            )
                    except IndexError:
                        raise HTTPException(
                            status_code=409,
                            detail=f"Incorrect else expression: {else_state}",
                        )
                else:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Values of variables: {names} do not match the formula. "
                        f"Alternatively, you can add an else expression.",
                    )
    else:
        try:
            node = ast.parse(param_type.constraint, mode="eval")
        except SyntaxError:
            raise HTTPException(
                status_code=400, detail="Could not parse formula"
            )
        names = sorted(
            {
                nd.slice.value
                for nd in ast.walk(node)
                if isinstance(nd, ast.Subscript)
            }
        )
        if names:
            values, parameter = evaluate_prm_value(
                session=session, names=names, mo_id=mo_id
            )
            pattern = r"parameter\['([^']+)'\]"
            tprm_names = re.findall(pattern, param_type.constraint)
            if tprm_names:
                formula = re.sub(
                    pattern, str(values.get(*tprm_names)), param_type.constraint
                )
        else:
            formula = param_type.constraint

    str_pattern = re.compile(r"'.+'")
    if str_pattern.fullmatch(formula):
        return formula[1:-1]
    elif formula == "True" or formula == "False":
        return formula
    try:
        node = ast.parse(formula, "<string>", mode="eval")
    except SyntaxError:
        raise HTTPException(status_code=422, detail="Could not parse formula")
    eval_names = sorted(
        {nd.id for nd in ast.walk(node) if isinstance(nd, ast.Name)}
    )
    if eval_names and not set(eval_names).issubset({"parameter", "x"}):
        return evaluate_formula(formula, vars={})
    slice_names = {nd for nd in ast.walk(node) if isinstance(nd, ast.Slice)}
    if slice_names:
        return evaluate_formula(formula, vars={})
    names = sorted({nd.s for nd in ast.walk(node) if isinstance(nd, ast.Str)})
    for name in names:
        if name != "x":
            param = session.exec(
                select(PRM)
                .join(TPRM)
                .where(
                    TPRM.name == name,
                    TPRM.val_type.in_(["float", "int"]),
                    TPRM.multiple != True,  # noqa
                    PRM.mo_id == mo_id,
                )
            ).first()
            if not param:
                param = session.exec(
                    select(PRM)
                    .join(TPRM)
                    .where(
                        TPRM.name == name.replace("_", " "),
                        TPRM.val_type.in_(["float", "int"]),
                        TPRM.multiple != True,  # noqa
                        PRM.mo_id == mo_id,
                    )
                ).first()
                if not param:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Int/float parameter with name '{name}' for"
                        f" object with id {mo_id} not found.",
                    )
            values[name] = float(param.value)
            parameter[name] = float(param.value)

    return evaluate_formula(formula, values, parameter)


def decode_multiple_value(value: Any):
    multiple_value_bytes = bytes.fromhex(value)
    multiple_value = pickle.loads(multiple_value_bytes)
    return multiple_value


def send_message_to_kafka(mes):
    pass


def update_object_type_attribute(
    session: Session, param_type: TPRM, db_object_type: TMO, attribute: str
):
    session.add(db_object_type)
    session.info["disable_security"] = True
    params_mo_id = session.exec(
        select(PRM.mo_id).where(PRM.tprm_id == param_type.id)
    ).all()
    all_mo = session.exec(select(MO).where(MO.id.in_([params_mo_id])))
    for mo in all_mo:
        setattr(mo, attribute, None)
        session.add(mo)


def set_param_attrs(
    session: Session,
    db_param: PRM,
    attr_value: Any,
    attribute: str,
    db_object: MO,
):
    if attribute == db_param.tprm_id:
        session.info["disable_security"] = True
        db_mo = session.get(MO, db_object.id)
        print(f"{db_mo.latitude=}, {db_mo.longitude=}")
        match db_param.tprm.val_type:
            case "float":
                setattr(db_mo, attr_value, float(db_param.value))
            case _:
                setattr(db_mo, attr_value, db_param.value)
        session.add(db_mo)


def set_location_attrs(
    session: Session,
    db_param: GeometryType,
    child_mos: list[MO],
    set_value: bool = False,
    location_data: dict | None = None,
):
    for current_mo in child_mos:  # type: MO
        match db_param.value:
            case "point":
                if set_value:
                    if location_data.get("latitude", None):
                        current_mo.latitude = location_data.get("latitude")
                    if location_data.get("longitude", None):
                        current_mo.longitude = location_data.get("longitude")
                else:
                    current_mo.latitude = current_mo.longitude = None
            case "line":
                if set_value:
                    if location_data.get("point_a_id", None):
                        current_mo.point_a_id = location_data.get("point_a_id")
                    if location_data.get("point_b_id", None):
                        current_mo.point_b_id = location_data.get("point_b_id")
                    if location_data.get("geometry", None):
                        current_mo.geometry = location_data.get("geometry")
                else:
                    current_mo.point_a_id = current_mo.point_b_id = (
                        current_mo.geometry
                    ) = None
            case "polygon":
                if set_value:
                    if location_data.get("geometry", None):
                        current_mo.geometry = location_data.get("geometry")
                else:
                    current_mo.geometry = None
        current_mo.version += 1
        session.add(current_mo)
    session.flush()


def extract_location_data(
    geometry_type: GeometryType,
    parent_mo: MO,
    new: bool = False,
    extra: dict | None = None,
) -> dict:
    result = {}
    if new:
        return extra

    match geometry_type.value:
        case "point":
            result["latitude"] = parent_mo.latitude
            result["longitude"] = parent_mo.longitude

        case "line":
            result["point_a_id"] = parent_mo.point_a_id
            result["point_b_id"] = parent_mo.point_b_id
            result["geometry"] = parent_mo.geometry

        case "polygon":
            result["geometry"] = parent_mo.geometry

        case _:
            raise ValueError("Incorrect parent MO data.")
    return result


def find_deep_parent(
    session: Session,
    object_type_instance: TMO,
    object_instance: MO,
    from_parent: bool = False,
) -> tuple[TMO, MO]:
    # From parent - if inherit_location not set yet need to set true
    # Looking for parent TMO and check inherit_location

    def find_tmo(
        expected_mo: MO, expected_tmo: TMO
    ) -> tuple[TMO, MO] | tuple[None, None]:
        if (
            not expected_tmo.inherit_location
            or not expected_tmo.p_id
            or not expected_mo.p_id
        ):
            return expected_tmo, expected_mo
        else:
            parent_mo: MO = session.execute(
                select(MO).where(MO.id == expected_mo.p_id)
            ).scalar()
            parent_tmo: TMO = session.execute(
                select(TMO).where(TMO.id == parent_mo.tmo_id)
            ).scalar()
            if parent_mo and parent_tmo:
                return find_tmo(expected_mo=parent_mo, expected_tmo=parent_tmo)
            else:
                return None, None

    if from_parent:
        cur_parent_tmo: TMO = session.get(TMO, object_type_instance.p_id)
        cur_parent_mo: MO = session.get(MO, object_instance.p_id)
    if from_parent and cur_parent_tmo and cur_parent_mo:
        tmo, mo = find_tmo(
            expected_mo=cur_parent_mo, expected_tmo=cur_parent_tmo
        )
        return tmo, mo
    else:
        return None, None


def evaluate_prm_value(
    session: Session, names: list, mo: MO, function_names: list, extra: dict
) -> (dict, dict):
    values: dict = {}
    parameter: dict = {}
    for i, name in enumerate(names):
        if name != "x" and function_names[i] == "parameter":
            param = session.exec(
                select(PRM)
                .join(TPRM)
                .where(
                    TPRM.name == name,
                    TPRM.multiple != True,  # noqa
                    PRM.mo_id == mo.id,
                )
            ).first()
            if not param:
                param = session.exec(
                    select(PRM)
                    .join(TPRM)
                    .where(
                        TPRM.name == name.replace("_", " "),
                        TPRM.multiple != True,  # noqa
                        PRM.mo_id == mo.id,
                    )
                ).first()
                if not param:
                    raise ValueError(
                        f"Single parameter with name '{name}' for"
                        f" object with id {mo.id} not found."
                    )
            val_type = param.tprm.val_type
            if val_type in ["int", "float"]:
                value = float(param.value)
            elif val_type == "bool":
                if param.value == "True":
                    value = True
                else:
                    value = False
            elif val_type == "mo_link":
                mo_link_value = session.exec(
                    select(MO.name).where(MO.id == int(param.value))
                ).first()
                value = str(mo_link_value)
            else:
                value = str(param.value)
        elif name != "x" and function_names[i] == "INNER_MAX":
            tprm = session.exec(
                select(TPRM)
                .outerjoin(PRM)
                .where(
                    TPRM.name == name,
                    TPRM.multiple != True,  # noqa
                    TPRM.tmo_id == mo.tmo_id,
                )
            ).first()
            if tprm:
                match tprm.val_type:
                    case "int" | "formula":
                        value = int(
                            max(
                                [prm.value for prm in tprm.prms],
                                default=extra.get("INNER_MAX_VALUE", 0) * -1,
                                key=int,
                            )
                        )
                    case _:
                        raise ValueError(
                            f"Incorrect tprm '{name}' val type for function {function_names[i]}"
                        )
            else:
                if extra.get("x"):
                    value = extra["x"]
                else:
                    raise ValueError(
                        f"Single parameter with name '{name}' for"
                        f" object with id {mo.id} not found."
                    )
        else:
            value = names[i]

        values[name] = value
        parameter[name] = value
    return values, parameter


def calculate_by_formula_new(
    session: Session, param_type: TPRM, object_instance: MO, x: Any = None
):
    # Check clause "if"/"without if", check then or else clause is need to calc
    # Replace TPRM name to value
    # need to additional eval (math, int, datetime)
    if not param_type.constraint:
        return ""
    extra_values = {}
    if x is not None:
        # if float(x) == int(x):
        #     values['x'] = int(x)
        # else:
        extra_values["x"] = float(x)
    # Formula contains if clause
    if ";" in param_type.constraint:
        # [str(if _cond_ then _action_)]
        statements = param_type.constraint.split(";")
        for i in range(0, len(statements) - 1):
            # find condition and then clause
            regex = re.compile(r"(?:if|elif) (.*) then (.*)")
            statement = statements[i]  # string: if _cond_ then _action_
            # (_cond_, _action_)
            expression = regex.findall(statement)[
                0
            ]  # tuple[string,string] _cond_, _action_
            if expression:
                """current_condition - parameter['TPRM'] == '1
                then_ - action for current condition '5'"""
                current_condition, then_ = expression
                if current_condition:
                    regex_in_condition = re.compile(
                        r"(.*?) (==|!=|>|>=|<=|<) (\w+)( or | and )?"
                    )
                    formula_tokens = regex_in_condition.findall(
                        current_condition
                    )
                    # Contains or/and clause
                    if formula_tokens and formula_tokens[0][3]:
                        all_bool_result: list[bool] = []
                        condition_names: list[str] = []
                        for token in formula_tokens:
                            left_condition = token[0]
                            clause_ = token[1]
                            right_condition = token[2]
                            condition_names.append(token[3])
                            temp_cond_result = _calc_condition(
                                session=session,
                                left_cond=left_condition,
                                right_cond=right_condition,
                                clause_=clause_,
                                mo=object_instance,
                                extra=extra_values,
                            )
                            if temp_cond_result is not None:
                                all_bool_result.append(temp_cond_result)
                            else:
                                continue
                        check_result: str = ""
                        for cur_name in range(len(condition_names)):
                            check_result += (
                                str(all_bool_result[cur_name])
                                + " "
                                + condition_names[cur_name]
                            )
                        condition_result: bool = eval(check_result)
                    # Without or/and clause
                    else:
                        regex_default = re.compile(
                            r"(.*) (==|!=|>|>=|<=|<) (.*)?"
                        )
                        # (left_condition, clause, right_condition)
                        # ("parameter['TPRM']", "==", "1")
                        data_for_eval: tuple = regex_default.findall(
                            current_condition
                        )[0]
                        if data_for_eval:
                            left_condition, clause_, right_condition = (
                                data_for_eval
                            )
                            condition_result = _calc_condition(
                                session=session,
                                left_cond=left_condition,
                                right_cond=right_condition,
                                clause_=clause_,
                                mo=object_instance,
                                extra=extra_values,
                            )
                            if condition_result is None:
                                continue
                else:
                    raise ValueError("Incorrect if statement.")
                # if we got True result then calculate value. Else check other condition
                if condition_result:
                    try:
                        result = formula_case(
                            session=session,
                            input_formula=then_,
                            mo=object_instance,
                            extra=extra_values,
                        )
                    except ValueError:
                        if param_type.required:
                            raise ValueError(
                                "You must add all prm value for required TPRM."
                            )
                        else:
                            result = ""
                    # Stop eval statements without calculate another clause elif
                    break
            else:
                raise ValueError("Incorrect if condition and action.")
        # Calculate last condition (else clause)
        else:
            if statements[-1]:
                regex = re.compile(r" else (.+)")
                condition = regex.findall(statements[-1])[0]
                try:
                    result = formula_case(
                        session=session,
                        input_formula=condition,
                        mo=object_instance,
                    )
                except ValueError:
                    if param_type.required:
                        raise ValueError(
                            "You must add all prm value for required TPRM."
                        )
                    else:
                        result = ""
            else:
                raise ValueError(
                    "Alternatively, you can add an else expression."
                )
    # Without if Simple case
    else:
        try:
            result = formula_case(
                session=session,
                input_formula=param_type.constraint,
                mo=object_instance,
                extra=extra_values,
            )
        except SyntaxError:
            raise ValueError("Could not parse formula")
        except ValueError as ex:
            if param_type.required:
                raise ValueError(
                    f"You must add all prm value for required TPRM {param_type.constraint}. {ex}"
                )
            else:
                result = ""
    return _correct_formula_result_type(result)


def formula_case(
    session: Session, input_formula: str, mo: MO, extra: dict = {}
) -> Any:
    values = {}
    parameter = {}
    try:
        node = ast.parse(input_formula, mode="eval")
        names = [
            nd.slice.value
            for nd in ast.walk(node)
            if isinstance(nd, ast.Subscript)
            and isinstance(nd.slice, ast.Constant)
        ]
        function_names = []
        for nd in ast.walk(node):
            if isinstance(nd, ast.Subscript) and isinstance(nd.value, ast.Name):
                function_names.append(nd.value.id)
            elif isinstance(nd, ast.Subscript) and isinstance(
                nd.value, ast.Call
            ):
                function_names.append(nd.value.func.attr)
        # For calc start value
        if (
            "INNER_MAX" in function_names
            and isinstance(node.body, ast.BinOp)
            and node.body.right
            and isinstance(node.body.right, ast.Constant)
        ):
            extra.update({"INNER_MAX_VALUE": node.body.right.n})
        if names:
            values, parameter = evaluate_prm_value(
                session=session,
                names=names,
                mo=mo,
                function_names=function_names,
                extra=extra,
            )
        if extra:
            values.update(extra)
            parameter.update(extra)
    except ValueError as ex:
        msg = f"Incorrect prm value with {names=} {values=} in mo. {ex}"
        raise ValueError(msg)
    except SyntaxError:
        raise SyntaxError("Could not parse formula")
    result = evaluate_formula(input_formula, values, parameter)
    return result


def calculate_by_formula_batch(
    session: Session,
    formula_tprm: TPRM,
    prm_data: dict[str, Any] | Series,
    tprms_from_formula_by_name: dict[str, TPRM],
) -> Any:
    """Calculate formula for batch without MO"""
    if not formula_tprm.constraint:
        return
    # Formula contains if clause
    if ";" in formula_tprm.constraint:
        # [str(if _cond_ then _action_)]
        statements = formula_tprm.constraint.split(";")
        for i in range(0, len(statements) - 1):
            # find condition and then clause
            regex = re.compile(r"(?:if|elif) (.*) then (.*)")
            statement = statements[i]  # string: if _cond_ then _action_
            # (_cond_, _action_)
            expression = regex.findall(statement)[
                0
            ]  # tuple[string,string] _cond_, _action_
            """current_condition - parameter['TPRM'] == '1
               then_ - action for current condition '5'"""
            current_condition, then_ = expression
            if current_condition:
                regex_in_condition = re.compile(
                    r"(.*?) (==|!=|>|>=|<=|<) (\w+)( or | and )?"
                )
                formula_tokens = regex_in_condition.findall(current_condition)
                # Contains or/and clause
                if formula_tokens and formula_tokens[0][3]:
                    all_bool_result: list[bool] = []
                    condition_names: list[str] = []
                    for token in formula_tokens:
                        left_condition, clause_, right_condition = token[0:3]
                        condition_names.append(token[3])
                        temp_cond_result = _calc_condition_batch(
                            session=session,
                            left_cond=left_condition,
                            right_cond=right_condition,
                            clause_=clause_,
                            tmo_id=formula_tprm.tmo_id,
                            prm_data=prm_data,
                        )
                        if temp_cond_result is not None:
                            all_bool_result.append(temp_cond_result)
                        else:
                            continue
                    check_result: str = ""
                    for cur_name in range(len(condition_names)):
                        check_result += (
                            str(all_bool_result[cur_name])
                            + " "
                            + condition_names[cur_name]
                        )
                    condition_result: bool = eval(check_result)
                # Without or/and clause
                else:
                    regex_default = re.compile(r"(.*) (==|!=|>|>=|<=|<) (.*)?")
                    # (left_condition, clause, right_condition)
                    # ("parameter['TPRM']", "==", "1")
                    data_for_eval: tuple = regex_default.findall(
                        current_condition
                    )[0]
                    if data_for_eval:
                        left_condition, clause_, right_condition = data_for_eval
                        condition_result = _calc_condition_batch(
                            session=session,
                            left_cond=left_condition,
                            right_cond=right_condition,
                            clause_=clause_,
                            tmo_id=formula_tprm.tmo_id,
                            prm_data=prm_data,
                            tprms_from_formula_by_name=tprms_from_formula_by_name,
                        )
                        if condition_result is None:
                            continue
            # if we got True result then calculate value. Else check other condition
            if condition_result:
                try:
                    result = formula_case_solver_batch(
                        session=session,
                        constraint=then_,
                        tmo_id=formula_tprm.tmo_id,
                        prm_data=prm_data,
                        tprms_from_formula_by_name=tprms_from_formula_by_name,
                    )
                except ValueError:
                    if formula_tprm.required:
                        raise ValueError(
                            "You must add all prm value for required TPRM."
                        )
                    else:
                        result = ""
                break
        # Calculate last condition (else clause)
        else:
            if statements[-1]:
                regex = re.compile(r" else (.+)")
                condition = regex.findall(statements[-1])[0]
                try:
                    result = formula_case_solver_batch(
                        session=session,
                        constraint=condition,
                        tmo_id=formula_tprm.tmo_id,
                        prm_data=prm_data,
                        tprms_from_formula_by_name=tprms_from_formula_by_name,
                    )
                except ValueError:
                    if formula_tprm.required:
                        raise ValueError(
                            "You must add all prm value for required TPRM."
                        )
                    else:
                        result = ""
            else:
                raise ValueError(
                    "Alternatively, you can add an else expression."
                )
    else:
        try:
            result = formula_case_solver_batch(
                session=session,
                constraint=formula_tprm.constraint,
                tmo_id=formula_tprm.tmo_id,
                prm_data=prm_data,
                tprms_from_formula_by_name=tprms_from_formula_by_name,
            )
        except ValueError:
            if formula_tprm.required:
                raise ValueError(
                    f"You must add all prm value for required TPRM {formula_tprm.constraint}."
                )
            else:
                result = ""

    return _correct_formula_result_type(result)


def formula_case_solver_batch(
    session: Session,
    constraint: str,
    tmo_id: int,
    prm_data: dict,
    tprms_from_formula_by_name: dict[str, TPRM],
) -> Any:
    values = {}
    parameter = {}
    extra = {}
    try:
        node = ast.parse(constraint, mode="eval")

        names = [
            nd.slice.value
            for nd in ast.walk(node)
            if isinstance(nd, ast.Subscript)
            and isinstance(nd.slice, ast.Constant)
        ]

        function_names = []
        for nd in ast.walk(node):
            if isinstance(nd, ast.Subscript) and isinstance(nd.value, ast.Name):
                function_names.append(nd.value.id)
            elif isinstance(nd, ast.Subscript) and isinstance(
                nd.value, ast.Call
            ):
                function_names.append(nd.value.func.attr)

        if (
            "INNER_MAX" in function_names
            and isinstance(node.body, ast.BinOp)
            and node.body.right
            and isinstance(node.body.right, ast.Constant)
        ):
            extra.update({"INNER_MAX_VALUE": node.body.right.n})

        if names:
            values, parameter = evaluate_prm_value_batch(
                session=session,
                names=names,
                function_names=function_names,
                prm_data=prm_data,
                extra=extra,
                tprm_by_name=tprms_from_formula_by_name,
            )

    except ValueError:
        msg = f"Incorrect prm value with {names=} {values=} in mo."
        print(msg)
        raise ValueError(msg)
    except SyntaxError:
        raise SyntaxError("Could not parse formula")
    result = evaluate_formula(constraint, values, parameter)
    return result


def evaluate_prm_value_batch(
    session: Session,
    names: list[str],
    function_names: list[str],
    prm_data: dict[str, Any],
    extra: dict,
    tprm_by_name: dict[str, TPRM],
) -> tuple[dict, dict]:
    if not names:
        return {}, {}
    if len(names) != len(function_names):
        raise ValueError("names and function_names must be the same length")

    param_names = [n for n, f in zip(names, function_names) if f == "parameter"]
    inner_max_names = [
        n for n, f in zip(names, function_names) if f == "INNER_MAX"
    ]
    needed_tprm_names = list({*param_names, *inner_max_names})

    if not needed_tprm_names:
        out = {n: n for n in names}
        return out, out.copy()

    missing = [n for n in needed_tprm_names if n not in tprm_by_name]
    if missing:
        raise ValueError(f"Single parameter(s) not found for names: {missing}")

    def _is_nan(x: Any) -> bool:
        return x is None or (isinstance(x, float) and math.isnan(x))

    mo_ids_needed = set()
    for n in param_names:
        t = tprm_by_name[n]
        if t.val_type == "mo_link":
            cur = prm_data.get(str(t.id), None)
            if not _is_nan(cur):
                try:
                    mo_ids_needed.add(int(cur))
                except Exception as e:
                    print(e)
                    pass

    mo_name_by_id: dict[int, str] = {}
    if mo_ids_needed:
        stmt_mo = select(MO.id, MO.name).where(MO.id.in_(list(mo_ids_needed)))
        for mo_id, mo_name in session.exec(stmt_mo).all():
            mo_name_by_id[mo_id] = mo_name

    def compute_inner_max(tprm: TPRM) -> int:
        if tprm.val_type not in ("int", "formula"):
            raise ValueError(
                f"Incorrect tprm '{tprm.name}' val type for function INNER_MAX"
            )
        default_base = int(extra.get("INNER_MAX_VALUE", 0)) * -1
        if not tprm.prms:
            return int(default_base)
        try:
            return int(
                max((int(p.value) for p in tprm.prms), default=default_base)
            )
        except Exception:
            vals = []
            for p in tprm.prms:
                try:
                    vals.append(int(p.value))
                except Exception as e:
                    print(e)
                    continue
            return int(max(vals)) if vals else int(default_base)

    values: dict[str, Any] = {}
    parameter: dict[str, Any] = {}

    for name, fn in zip(names, function_names):
        value = name

        if fn == "parameter":
            t = tprm_by_name[name]
            value = prm_data.get(str(t.id), None)
        elif fn == "INNER_MAX":
            t = tprm_by_name[name]
            value = compute_inner_max(t)

        values[name] = value
        parameter[name] = value
    return values, parameter


def _calc_condition(
    session: Session,
    left_cond: str,
    right_cond: str,
    clause_: str,
    mo: MO,
    extra: dict,
) -> bool | None:
    """Calc left and right conditions in the statement, then eval bool result for statement"""
    try:
        left_value = formula_case(
            session=session, input_formula=left_cond, mo=mo, extra=extra
        )
        right_value = formula_case(
            session=session, input_formula=right_cond, mo=mo, extra=extra
        )
    except ValueError:
        return
    try:
        if isinstance(right_value, str):
            cond_result = eval(f"'{left_value}'" + clause_ + f"'{right_value}'")
        elif isinstance(left_value, datetime) and isinstance(
            right_value, datetime
        ):
            cond_result = eval(
                str(datetime.timestamp(left_value))
                + clause_
                + str(datetime.timestamp(right_value))
            )
        else:
            cond_result = eval(str(left_value) + clause_ + str(right_value))
    except SyntaxError:
        return
    return cond_result


def _calc_condition_batch(
    session: Session,
    left_cond: str,
    right_cond: str,
    clause_: str,
    tmo_id: int,
    prm_data: dict,
    tprms_from_formula_by_name: dict[str, TPRM],
) -> bool | None:
    """Calc left and right conditions in the statement, then eval bool result for statement"""
    try:
        left_value = formula_case_solver_batch(
            session=session,
            constraint=left_cond,
            tmo_id=tmo_id,
            prm_data=prm_data,
            tprms_from_formula_by_name=tprms_from_formula_by_name,
        )
        right_value = formula_case_solver_batch(
            session=session,
            constraint=right_cond,
            tmo_id=tmo_id,
            prm_data=prm_data,
            tprms_from_formula_by_name=tprms_from_formula_by_name,
        )
    except ValueError:
        return
    try:
        if isinstance(right_value, str):
            cond_result = eval(f"'{left_value}'" + clause_ + f"'{right_value}'")
        elif isinstance(left_value, datetime) and isinstance(
            right_value, datetime
        ):
            cond_result = eval(
                str(datetime.timestamp(left_value))
                + clause_
                + str(datetime.timestamp(right_value))
            )
        else:
            cond_result = eval(str(left_value) + clause_ + str(right_value))
    except SyntaxError:
        return
    return cond_result


def _correct_formula_result_type(result: Any):
    if result is None:
        result = ""
    elif isinstance(result, datetime):
        pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
        result = datetime.strftime(result, pattern)
    elif isinstance(result, timedelta):
        result = result.total_seconds()
    return result
