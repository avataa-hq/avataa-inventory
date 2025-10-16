class ObjectCustomException(Exception):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail

    def __str__(self):
        if self.status_code:
            return f"[Error {self.status_code}]: {self.detail}"
        return self.detail


class DescendantsLimit(ObjectCustomException):
    pass


class ObjectNotExists(ObjectCustomException):
    pass


class NotActualVersion(ObjectCustomException):
    pass


class UpdatedObjectDataHasNoDifferenceWithOriginal(ObjectCustomException):
    pass


class ObjectCanNotBeArchived(ObjectCustomException):
    pass


class ObjectCanNotBeParent(ObjectCustomException):
    pass


class DuplicatedObjectName(ObjectCustomException):
    pass


class CanNotDeletePrimaryObject(ObjectCustomException):
    pass


class ParentNotMatchToObjectTypeParent(ObjectCustomException):
    pass


class PointInstanceNotExists(ObjectCustomException):
    pass


class DuplicatedParameter(ObjectCustomException):
    pass
