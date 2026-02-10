from typing import Any


def dedupe_events(events: list[dict[str, Any]], key: str = "event_id") -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for event in events:
        event_id = event.get(key)
        if not isinstance(event_id, str):
            output.append(event)
            continue
        if event_id in seen:
            continue
        seen.add(event_id)
        output.append(event)
    return output
