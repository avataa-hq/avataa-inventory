class ParameterTypeCustomException(Exception):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class ParameterTypeNotExists(ParameterTypeCustomException):
    pass


class ParameterTypeNotValidForPrimary(ParameterTypeCustomException):
    pass


class ParameterTypeNotValidForLabel(ParameterTypeCustomException):
    pass


class ParameterTypeNotValidForStatus(ParameterTypeCustomException):
    pass


class ParameterTypeNameIsReserved(ParameterTypeCustomException):
    pass


class NotValidParameterTypeName(ParameterTypeCustomException):
    pass


class NotValidParameterTypePreferences(ParameterTypeCustomException):
    pass


class ThisValueTypeNeedConstraint(ParameterTypeCustomException):
    pass
