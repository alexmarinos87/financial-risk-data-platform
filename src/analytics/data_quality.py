def late_rate(late_count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return late_count / total
