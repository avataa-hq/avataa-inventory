from routers.parameter_type_router.exceptions import (
    ParameterTypeCustomException,
)


class EnumValTypeCustomExceptions(ParameterTypeCustomException):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class NotExistsConstraint(EnumValTypeCustomExceptions): ...


class NotValidConstraint(EnumValTypeCustomExceptions): ...


class FieldValueMissing(EnumValTypeCustomExceptions): ...


class FieldValueNotValidWithConstraint(EnumValTypeCustomExceptions): ...


class DuplicatedTPRMNameInRequest(EnumValTypeCustomExceptions): ...


class TPRMWithThisNameAlreadyExists(EnumValTypeCustomExceptions): ...


class NotFoundParameterType(EnumValTypeCustomExceptions): ...


class ForceIsNotActivated(EnumValTypeCustomExceptions): ...


class VersionIsNotActual(EnumValTypeCustomExceptions): ...


class ParameterValueNotValidWithConstraint(EnumValTypeCustomExceptions): ...


class ParameterAlreadyExists(EnumValTypeCustomExceptions): ...


class ParameterNotExists(EnumValTypeCustomExceptions): ...
