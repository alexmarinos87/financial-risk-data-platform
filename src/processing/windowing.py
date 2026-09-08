from datetime import datetime, timedelta, timezone

from src.common.exceptions import ValidationError


def floor_time(ts: datetime, minutes: int) -> datetime:
    """Floor fixed-duration windows to the Unix epoch without float truncation.

    Aware timestamps share a UTC anchor and retain their display timezone.
    Naive timestamps retain the historical naive-epoch behaviour.
    """
    if not isinstance(ts, datetime):
        raise ValidationError("Window timestamp must be a datetime")
    if type(minutes) is not int or minutes <= 0:
        raise ValidationError("Window minutes must be a positive integer")
    aware = ts.tzinfo is not None and ts.utcoffset() is not None
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc if aware else None)
    try:
        value = ts.astimezone(timezone.utc) if aware else ts.replace(tzinfo=None)
        interval = timedelta(minutes=minutes)
        # Timedelta floor division preserves fractions before the epoch and
        # avoids floating-point rounding for distant dates.
        rounded = epoch + int((value - epoch) // interval) * interval
        return rounded.astimezone(ts.tzinfo) if aware else rounded
    except OverflowError:
        raise ValidationError("Window duration or boundary is outside datetime range") from None
