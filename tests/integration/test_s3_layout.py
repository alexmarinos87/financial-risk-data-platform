from datetime import datetime, timezone
from src.storage.partitioning import partition_path


def test_partition_path():
    ts = datetime(2025, 1, 1, 1, 0, tzinfo=timezone.utc)
    assert partition_path(ts) == "year=2025/month=01/day=01/hour=01"
