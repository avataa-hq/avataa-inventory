class BatchCustomException(Exception):
    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class RequestedTMOIsVirtual(BatchCustomException):
    pass


class NotAllowedFileType(BatchCustomException):
    pass


class FileReadingException(BatchCustomException):
    pass


class NotUniqueColumnsInFile(BatchCustomException):
    pass


class NotExistsTPRMsInHeader(BatchCustomException):
    pass


class NotAddedRequiredAttributes(BatchCustomException):
    pass


class NotExistsMOAttributes(BatchCustomException):
    pass


class ColumnValuesValidationError(BatchCustomException):
    pass


class DuplicatedMONameInFile(BatchCustomException):
    pass


class SequenceNotImplemented(BatchCustomException):
    pass


class TMONotExists(BatchCustomException):
    pass
