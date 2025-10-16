class MigrationException(Exception):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class NotValidFileContentType(MigrationException):
    pass


class NotValidValue(MigrationException):
    pass


class NotValidAttribute(MigrationException):
    pass


class ObjectTypeAlreadyExists(MigrationException):
    pass


class ObjectTypeNotExists(MigrationException):
    pass
