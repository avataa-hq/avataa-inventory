from fastapi import HTTPException
import ast
import operator
from typing import Any, Dict

import math
import datetime


def byte_offset_to_char_offset(source: str, byte_offset: int) -> int:
    while True:
        try:
            pre_source = source.encode()[:byte_offset].decode()
            break
        except UnicodeDecodeError:
            byte_offset -= 1
            continue
    return len(pre_source)


class FormulaError(Exception):
    pass


class FormulaSyntaxError(FormulaError):
    def __init__(self, msg: str, lineno: int, offset: int):
        self.msg = msg
        self.lineno = lineno
        self.offset = offset

    @classmethod
    def from_ast_node(
        cls, source: str, node: ast.AST, msg: str
    ) -> "FormulaSyntaxError":
        lineno = node.lineno
        col_offset = node.col_offset
        offset = byte_offset_to_char_offset(source, col_offset)
        return cls(msg=msg, lineno=lineno, offset=offset + 1)

    @classmethod
    def from_syntax_error(
        cls, error: SyntaxError, msg: str
    ) -> "FormulaSyntaxError":
        return cls(
            msg=f"{msg}: {error.msg}", lineno=error.lineno, offset=error.offset
        )

    def __str__(self):
        return f"{self.lineno}:{self.offset}: {self.msg}"


class FormulaRuntimeError(FormulaError):
    pass


MAX_FORMULA_LENGTH = 255


def evaluate_formula(
    formula: str, variables: Dict[str, Any], parameters: dict = None
) -> float:
    if not parameters:
        parameters = dict()
    if len(formula) > MAX_FORMULA_LENGTH:
        raise HTTPException(status_code=422, detail="The formula is too long")

    try:
        for index, (parameter, value) in enumerate(parameters.items()):
            formula = formula.replace(
                f"parameter['{parameter}']", f"var_{index}"
            )
            formula = formula.replace(
                f"INNER_MAX['{parameter}']", f"var_{index}"
            )
            variables[f"var_{index}"] = value
        node = ast.parse(formula, "<string>", mode="eval")
    except SyntaxError:
        raise HTTPException(status_code=422, detail="Could not parse formula")

    try:
        return eval_node(formula, node, variables, parameters)
    except HTTPException:
        raise
    except Exception as ex:
        print(f"Formula evaluation failed: {ex}")


def eval_node(
    source: str, node: ast.AST, vars: Dict[str, Any], parameter: dict = None
) -> float:
    EVALUATORS = {
        ast.Expression: eval_expression,
        ast.Constant: eval_constant,
        ast.Name: eval_name,
        ast.BinOp: eval_binop,
        ast.UnaryOp: eval_unaryop,
        ast.Subscript: eval_subscript,
        ast.Call: eval_call,
        ast.BoolOp: eval_bool,
        ast.Compare: eval_compare,
        ast.Attribute: eval_attribute,
    }

    for ast_type, evaluator in EVALUATORS.items():
        if isinstance(node, ast_type):
            return evaluator(source, node, vars, parameter)

    raise HTTPException(
        status_code=422, detail="This formula syntax is not supported"
    )


def eval_expression(
    source: str, node: ast.Expression, vars: Dict[str, Any], parameter
) -> float:
    return eval_node(source, node.body, vars, parameter)


def eval_constant(
    source: str, node: ast.Constant, vars: Dict[str, Any], parameter
) -> float:
    if isinstance(node.value, int) or isinstance(node.value, float):
        int_value = int(node.value)
        if int_value == float(node.value):
            return int(node.value)
        return float(node.value)
    else:
        return node.value


def eval_name(
    source: str, node: ast.Name, vars: Dict[str, Any], parameter
) -> float | str | datetime.datetime:
    try:
        int_value = int(vars[node.id])
        float_value = float(vars[node.id])
        if int_value != vars[node.id] or float_value != vars[node.id]:
            return vars[node.id]
        if int_value == float_value:
            return int_value
        return float(vars[node.id])
    except ValueError:
        pass
    try:
        # 2024-03-01T17:23:14.907907Z'
        pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
        return datetime.datetime.strptime(vars[node.id], pattern)
    except ValueError:
        pass
    return vars[node.id]


def eval_binop(
    source: str, node: ast.BinOp, vars: Dict[str, Any], parameter
) -> float:
    OPERATIONS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
    }

    left_value = eval_node(source, node.left, vars, parameter)
    right_value = eval_node(source, node.right, vars, parameter)

    try:
        apply = OPERATIONS[type(node.op)]
    except KeyError:
        raise HTTPException(
            status_code=422, detail="Operations of this type are not supported"
        )
    # For correct string concatenation
    if isinstance(left_value, str) or isinstance(right_value, str):
        left_value = str(left_value)
        right_value = str(right_value)
    return apply(left_value, right_value)


def eval_unaryop(
    source: str, node: ast.UnaryOp, vars: Dict[str, Any], parameter
) -> float:
    OPERATIONS = {
        ast.USub: operator.neg,
    }

    operand_value = eval_node(source, node.operand, vars, parameter)

    try:
        apply = OPERATIONS[type(node.op)]
    except KeyError:
        raise FormulaSyntaxError.from_ast_node(
            source, node, "Operations of this type are not supported"
        )

    return apply(operand_value)


def eval_subscript(
    source: str, node: ast.Subscript, variables: Dict[str, Any], parameter
) -> Any:
    try:
        if isinstance(node.value, ast.Constant):
            return node.value.s[
                node.slice.lower.s
                if node.slice.lower
                else 0 : node.slice.upper.s
                if node.slice.upper
                else None : node.slice.step.s if node.slice.step else None
            ]
        start = stop = step = None
        # Slice in string
        if isinstance(node.slice, ast.Constant):
            start = node.slice.s
        elif isinstance(node.slice, ast.Slice):
            if isinstance(node.slice.lower, ast.UnaryOp):
                start = eval_unaryop(
                    source, node.slice.lower, variables, parameter
                )
            elif node.slice.lower:
                start = node.slice.lower.s
            else:
                start = 0
            if isinstance(node.slice.upper, ast.UnaryOp):
                stop = eval_unaryop(
                    source, node.slice.upper, variables, parameter
                )
            elif node.slice.upper:
                stop = node.slice.upper.s
            else:
                stop = None
            if isinstance(node.slice.step, ast.UnaryOp):
                step = eval_unaryop(
                    source, node.slice.step, variables, parameter
                )
            elif node.slice.step:
                step = node.slice.step.s
            else:
                step = None
        if isinstance(node.value, ast.Name):
            if node.value.id in ["parameter", "INNER_MAX"]:
                return float(variables[node.value.id])
            result = variables[node.value.id][start:stop:step]
            return result
        if isinstance(node.value, ast.Subscript):
            result_subscript = eval_subscript(
                source, node.value, variables, parameter
            )
            result = result_subscript[start:stop:step]
            return result
        if isinstance(node.value, ast.Call):
            if node.slice is not None and isinstance(node.slice, ast.Constant):
                call_result = eval_call(
                    source, node.value, variables, parameter
                )
                result = call_result[node.slice.s]
            else:
                result = eval_call(source, node.value, variables, parameter)
            return result
        else:
            return float(variables[node.value.id])
    except KeyError:
        if node.value.id == "parameter":
            msg = "Incorrect formula. Remove double quotes."
        else:
            msg = f"Undefined variable: {node.value.id}"
        raise HTTPException(status_code=422, detail=msg)


def eval_call(
    source: str, node: ast.Call, vars: Dict[str, Any], parameter: Dict
) -> Any:
    try:
        if isinstance(node.func, ast.Name):
            allowed_function = ["int", "str"]
            if node.func.id in allowed_function:
                result = eval(ast.unparse(node), vars)
                return result
        elif isinstance(node.func, ast.Attribute):
            functions_allowed = {
                "math": math,
                "datetime": datetime,
                "split": str,
            }
            # math
            func_values = []
            if node.func.attr == "split":
                # func_values.append(str(eval_subscript(source, node.func.value, vars, parameter)))
                if isinstance(node.func.value, ast.Subscript):
                    subscript_result = eval_subscript(
                        source, node.func.value, vars, parameter
                    )
                    return getattr(subscript_result, node.func.attr)(
                        *[
                            arg.s
                            for arg in node.args
                            if isinstance(arg, ast.Constant)
                        ]
                    )
                    # return getattr(subscript_result, node.func.attr)()
                elif isinstance(node.func.value, ast.Name):
                    return getattr(
                        vars.get(node.func.value.id), node.func.attr
                    )()
                elif isinstance(node.func.value, ast.Call):
                    value = eval_call(source, node.func.value, vars, parameter)
                    return value
            elif node.args:
                for arg in node.args:
                    # Number
                    if isinstance(arg, ast.Constant):
                        if not isinstance(
                            node.func, ast.Attribute
                        ) and isinstance(node.value.value, ast.Call):
                            func_values.append(
                                eval_call(source, node.value.value, vars)
                            )
                        else:
                            func_values.append(arg.s)
                    # Parameter
                    elif isinstance(arg, ast.Name):
                        func_values.append(vars[arg.id])
                    # elif validate formula
                    elif isinstance(arg, ast.Subscript):
                        func_values.append(1)
                    else:
                        raise SyntaxError("Incorrect formula.")
            # datetime.timedelta(days=7)
            else:
                if node.keywords:
                    value = {}
                    for arg in node.keywords:
                        if isinstance(arg.value, ast.Constant):
                            value[arg.arg] = arg.value.value
                    result = getattr(
                        functions_allowed.get(node.func.value.id),
                        node.func.attr,
                    )(**value)
                    return result
                elif node.func.attr:
                    result = getattr(
                        getattr(
                            functions_allowed.get(node.func.value.id),
                            node.func.value.id,
                        ),
                        node.func.attr,
                    )()
                    return result

            result = getattr(
                functions_allowed.get(node.func.value.id), node.func.attr
            )(*func_values)
            return result
    except KeyError:
        raise HTTPException(
            status_code=422, detail=f"Undefined variable: {node.func.id}"
        )


def eval_bool(
    source: str, node: ast.BoolOp, vars: Dict[str, Any], parameter: Dict
) -> Any:
    pass
    # operands = []
    # for val in node.values:
    #     if isinstance(val, ast.Name):
    #         operands.append(vars[val.id])
    #     elif isinstance(val, ast.Compare):
    #         comparator_result = eval_node(source, val, vars, parameter)
    #         for el in comparator_result:  # type: bool
    #             pass
    # return "1"


def eval_attribute(
    source: str, node: ast.Attribute, vars: Dict[str, Any], parameter: Dict
) -> Any:
    raise NotImplementedError


def eval_compare(
    source: str, node: ast.Compare, vars: Dict[str, Any], parameter: Dict
) -> Any:
    pass
    # OPERATIONS = {
    #     ast.Eq: operator.eq,
    # }
    # result = []
    # for i, op in enumerate(node.ops):
    #     try:
    #         apply = OPERATIONS[type(op)]
    #     except KeyError:
    #         raise FormulaSyntaxError.from_ast_node(source, node, "Operations of this type are not supported")
    #     if isinstance(node.left, ast.Name) and isinstance(node.comparators[i], ast.Constant):
    #         result.append(apply(node.left.id, node.comparators[0]))
    # return result
