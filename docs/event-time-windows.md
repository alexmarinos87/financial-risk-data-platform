# Fixed-duration event-time windows

Primary arc42 building block: `processing`.

`src/processing/windowing.py::floor_time` assigns event instants to fixed-duration
windows anchored at 1970-01-01 00:00:00 UTC. For aware datetimes it calculates in
UTC and converts the boundary back to the input timezone. The same instant has
the same window regardless of its displayed offset. Daylight-saving folds are
therefore distinct instants, not a reason to merge or shift hourly windows.

The implementation uses exact timedelta floor division. Converting fractional
elapsed seconds with `int()` truncated towards zero and could assign an event
just before the Unix epoch to a boundary after the event. For example:

```text
1969-12-31T23:59:59.999999Z, five-minute window
  -> 1969-12-31T23:55:00Z
```

The interval must be a positive Python integer; booleans, fractional values,
zero and negative values are rejected. Unrepresentable durations or boundaries
raise `ValidationError`. Ordinary five-minute UTC windows retain their results.
Naive datetimes retain the existing naive-epoch utility behaviour, but this does
not relax the ingestion model's timezone-aware timestamp requirement.

These are elapsed-time windows, not local business sessions or calendar days.
A 1,440-minute window is a UTC-aligned 24-hour duration, not a daylight-saving
local day. Direct callers that previously relied on timezone-local epoch
alignment must explicitly use a separate calendar/session policy instead.

Run the boundary and timezone regressions with:

```bash
python -m pytest -q tests/unit/test_windowing.py \
  tests/unit/test_windowing_timestamp_compatibility.py
```

The tests cover fractional pre-epoch values, non-whole-hour timezone offsets,
London clock changes, multi-hour/day intervals, containment, alignment,
idempotence and invalid inputs. No external data, provider or infrastructure is
required; clock-change cases use the runtime's timezone database.


## Timestamp representation compatibility

Window assignment uses built-in `datetime` and `timedelta` arithmetic even when
its input is a pandas `Timestamp`. The same instant represented in seconds,
milliseconds, microseconds or nanoseconds must not change the result or impose a
representation-specific duration ceiling. For example, a 200,000,000-minute
interval fits Python's duration range but exceeds a nanosecond timedelta's range;
this is an arithmetic-boundary regression case, not a recommended risk window.

The conversion retains the calendar fields, timezone and daylight-saving `fold`.
A submicrosecond remainder cannot affect an integer-minute window: every UTC
boundary is microsecond-aligned, and truncating that positive remainder never
crosses such a boundary, including before 1970. No float timestamp conversion or
rounding is used. This is window assignment only, not source-event timestamp
serialization. The input is not mutated and the result is a built-in `datetime`.

Missing pandas timestamps (`NaT`) raise the fixed public `ValidationError`
instead of exposing a backend arithmetic error. Durations or resulting UTC/local
boundaries genuinely outside Python's representable range remain rejected.
The tests cover representation parity, exact nanosecond edges, both London folds,
the supported datetime endpoints and actual boundary underflow. The normal
five-minute pipeline policy, ingestion contract and dependencies are unchanged.
