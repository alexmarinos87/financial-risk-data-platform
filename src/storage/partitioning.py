from datetime import datetime


def partition_path(ts: datetime) -> str:
    return f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"
