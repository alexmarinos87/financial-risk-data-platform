from typing import Any

from src.common.exceptions import ValidationError


def dedupe_events(events: list[dict[str, Any]], key: str = "event_id") -> list[dict[str, Any]]:
    """Collapse equal records, rejecting conflicting reuse of an event identity.

    Callers should normalise records before deduplication. The first occurrence
    and input order are preserved; neither the input list nor its records change.
    """
    seen: dict[Any, dict[str, Any]] = {}
    output: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        if key not in event or event[key] is None:
            raise ValidationError(f"Event at index {index} has no deduplication identity")
        event_id = event[key]
        try:
            hash(event_id)
        except TypeError:
            raise ValidationError(
                f"Event at index {index} has an unhashable deduplication identity"
            ) from None
        if event_id in seen:
            if event != seen[event_id]:
                # Do not expose the event ID or payload in validation errors.
                raise ValidationError(f"Conflicting duplicate event identity at index {index}")
            continue
        seen[event_id] = event
        output.append(event)
    return output
