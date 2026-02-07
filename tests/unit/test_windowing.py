from datetime import datetime, timezone
from src.processing.windowing import floor_time


def test_floor_time():
    ts = datetime(2025, 1, 1, 12, 7, tzinfo=timezone.utc)
    rounded = floor_time(ts, 5)
    assert rounded.minute == 5
