from ..common.exceptions import ValidationError


def require_fields(payload: dict, fields: list[str]) -> None:
    missing = [f for f in fields if f not in payload]
    if missing:
        raise ValidationError(f"Missing required fields: {missing}")
