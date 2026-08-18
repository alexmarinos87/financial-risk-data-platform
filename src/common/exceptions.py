class ValidationError(Exception):
    pass


class IngestionError(Exception):
    pass


class StorageError(Exception):
    pass


class RawEventConflictError(StorageError):
    pass


class OverlapError(Exception):
    pass
