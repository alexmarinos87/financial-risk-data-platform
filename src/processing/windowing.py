from datetime import datetime, timedelta


def floor_time(ts: datetime, minutes: int) -> datetime:
    delta = timedelta(minutes=minutes)
    epoch = datetime(1970, 1, 1, tzinfo=ts.tzinfo)
    seconds = int((ts - epoch).total_seconds())
    return epoch + timedelta(seconds=(seconds // int(delta.total_seconds())) * int(delta.total_seconds()))
