from __future__ import annotations

from collections.abc import Hashable
from typing import Any


def dedupe_events(events: list[dict[str, Any]], key: str = "event_id") -> list[dict[str, Any]]:
    seen: set[Hashable] = set()
    output: list[dict[str, Any]] = []
    for event in events:
        event_id = event.get(key)
        if not isinstance(event_id, Hashable):
            output.append(event)
            continue
        if event_id in seen:
            continue
        seen.add(event_id)
        output.append(event)
    return output
