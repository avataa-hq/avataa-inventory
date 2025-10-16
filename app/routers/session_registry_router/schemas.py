from datetime import datetime

from pydantic import BaseModel, model_validator


class GetSessionRegistryByUserIdRequest(BaseModel):
    user_id: str | None = None
    limit: int = 10
    offset: int = 0
    datetime_from: datetime | None = None
    datetime_to: datetime | None = None

    @model_validator(mode="after")
    def validate_datetime(self):
        if self.datetime_from is not None and self.datetime_to is not None:
            if self.datetime_from > self.datetime_to:
                raise ValueError("Datetime from must be less than datetime to")

        return self


class SessionRegistryRequest(BaseModel):
    limit: int = 10
    offset: int = 0
    datetime_from: datetime | None = None
    datetime_to: datetime | None = None

    @model_validator(mode="after")
    def validate_datetime(self):
        if self.datetime_from is not None and self.datetime_to is not None:
            if self.datetime_from > self.datetime_to:
                raise ValueError("Datetime from must be less than datetime to")

        return self


class GetSessionRegistryByUserIdResponse(BaseModel):
    response_data: list
    total: int
