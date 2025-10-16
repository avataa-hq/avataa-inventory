class ParameterCustomException(Exception):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail

    def __str__(self):
        if self.status_code:
            return f"[Error {self.status_code}]: {self.detail}"
        return self.detail


class ParameterNotExists(ParameterCustomException):
    pass


class ParameterTypeNotExists(ParameterCustomException):
    pass


class PrimaryTPRMParameterError(ParameterCustomException):
    pass


class ParametersAlreadyExistError(ParameterCustomException):
    pass


class NotListParameterError(ParameterCustomException):
    pass


class CannotCreateForSequenceTypeError(ParameterCustomException):
    pass


class NotValidParameterVersion(ParameterCustomException):
    pass


class NotValidSequenceValue(ParameterCustomException):
    pass


class NotValidParameterValue(ParameterCustomException):
    pass


class NotValidParameterFilter(ParameterCustomException):
    pass


class ValTypeValidationNotImplemented(ParameterCustomException):
    pass


class NotValidFormulaParameterValue(ParameterCustomException):
    pass
