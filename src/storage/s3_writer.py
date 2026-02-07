from ..common.exceptions import StorageError


def write_records(records: list[dict]) -> int:
    if records is None:
        raise StorageError("No records provided")
    return len(records)
