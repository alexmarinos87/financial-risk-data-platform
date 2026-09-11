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
    try:
        # Use built-in arithmetic, not a subclass's resolution-limited delta.
        # Minute boundaries are microsecond-aligned: dropping a positive
        # submicrosecond remainder cannot change which window contains ts.
        value = datetime(
            ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second,
            ts.microsecond, tzinfo=ts.tzinfo, fold=ts.fold,
        )
        aware = value.tzinfo is not None and value.utcoffset() is not None
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc if aware else None)
        value = value.astimezone(timezone.utc) if aware else value.replace(tzinfo=None)
        interval = timedelta(minutes=minutes)
        # Timedelta floor division preserves fractions before the epoch and
        # avoids floating-point rounding for distant dates.
        rounded = epoch + int((value - epoch) // interval) * interval
        return rounded.astimezone(ts.tzinfo) if aware else rounded
    except (TypeError, ValueError):
        raise ValidationError("Window timestamp must be a valid datetime") from None
    except OverflowError:
        raise ValidationError("Window duration or boundary is outside datetime range") from None
