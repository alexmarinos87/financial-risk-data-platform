from datetime import datetime, timezone


def partition_path(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)
    return f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"
