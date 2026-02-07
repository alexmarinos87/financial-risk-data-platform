def dedupe_events(events: list[dict], key: str = "event_id") -> list[dict]:
    seen: set[str] = set()
    output: list[dict] = []
    for event in events:
        event_id = event.get(key)
        if event_id in seen:
            continue
        seen.add(event_id)
        output.append(event)
    return output
