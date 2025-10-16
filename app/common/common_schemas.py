from typing import Annotated

from pydantic import BaseModel, AfterValidator


def order_by_rule_validator(value):
    if value not in ["asc", "desc"]:
        raise ValueError("Rule must equals one of values: asc, desc")

    return value


class ErrorResponseModel(BaseModel):
    error: str


class OrderByRule(BaseModel):
    rule: Annotated[str, AfterValidator(order_by_rule_validator)] = "asc"
