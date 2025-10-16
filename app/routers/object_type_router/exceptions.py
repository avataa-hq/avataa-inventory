class ObjectTypeCustomException(Exception):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class ObjectTypeNotExists(ObjectTypeCustomException):
    pass


class ObjectTypeAlreadyExists(ObjectTypeCustomException):
    pass


class ObjectTypeHasNoParent(ObjectTypeCustomException):
    pass


class ObjectTypeIsVirtual(ObjectTypeCustomException):
    pass


class ObjectTypeNotValidAsConstraint(ObjectTypeCustomException):
    pass


class NotValidLifecycleProcessDefinition(ObjectTypeCustomException):
    pass


class NotActualVersion(ObjectTypeCustomException):
    pass


class SeverityIdNotValid(ObjectTypeCustomException):
    pass


class PrimaryNotValid(ObjectTypeCustomException):
    pass


class LabelNotValid(ObjectTypeCustomException):
    pass


class GeometryNotValid(ObjectTypeCustomException):
    pass
