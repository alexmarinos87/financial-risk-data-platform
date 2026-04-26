from datetime import datetime, timedelta, timezone

from src.storage.partitioning import partition_path


def test_partition_path():
    ts = datetime(2025, 1, 1, 1, 0, tzinfo=timezone.utc)
    assert partition_path(ts) == "year=2025/month=01/day=01/hour=01"


def test_partition_path_normalizes_to_utc():
    ts = datetime(2025, 1, 1, 1, 30, tzinfo=timezone(timedelta(hours=2)))
    assert partition_path(ts) == "year=2024/month=12/day=31/hour=23"
