from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.config import load_yaml
from src.common.exceptions import StorageError, ValidationError

MODEL_VERSION = "portfolio-risk-notification-destination-v1"
CHANNEL = "webhook"
MAX_DESTINATIONS = 20
MAX_PURPOSE_LENGTH = 256
MAX_REVIEW_DAYS = 366
EVENT_TYPES = frozenset(
    {
        "breach_opened",
        "breach_escalated",
        "breach_deescalated",
        "breach_resolved",
    }
)
SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")


@dataclass(frozen=True, slots=True)
class DestinationOwner:
    team: str
    contact: str


@dataclass(frozen=True, slots=True)
class DestinationActivation:
    enabled: bool
    change_request_id: str | None
    reviewed_by: tuple[str, ...]
    reviewed_at: datetime | None
    review_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class NotificationDestination:
    destination_id: str
    channel: str
    endpoint_env: str
    owner: DestinationOwner
    purpose: str
    recipient_scope: str
    data_classification: str
    allowed_event_types: tuple[str, ...]
    activation: DestinationActivation

    @property
    def fingerprint(self) -> str:
        payload = {
            "activation": {
                "change_request_id": self.activation.change_request_id,
                "enabled": self.activation.enabled,
                "review_expires_at": _iso(self.activation.review_expires_at),
                "reviewed_at": _iso(self.activation.reviewed_at),
                "reviewed_by": list(self.activation.reviewed_by),
            },
            "allowed_event_types": list(self.allowed_event_types),
            "channel": self.channel,
            "data_classification": self.data_classification,
            "destination_id": self.destination_id,
            "endpoint_env": self.endpoint_env,
            "model_version": MODEL_VERSION,
            "owner": {
                "contact": self.owner.contact,
                "team": self.owner.team,
            },
            "purpose": self.purpose,
            "recipient_scope": self.recipient_scope,
        }
        digest = hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"{MODEL_VERSION}-destination-{digest}"


def _exact_mapping(value: Any, label: str, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    actual = set(value)
    if actual != keys:
        missing = sorted(keys - actual)
        unknown = sorted(actual - keys)
        raise ValidationError(
            f"{label} fields are invalid; missing={missing}, unknown={unknown}"
        )
    return value


def _segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _optional_segment(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _segment(value, label)


def _purpose(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValidationError("purpose must be canonical non-empty text")
    if len(value) > MAX_PURPOSE_LENGTH or any(ord(character) < 32 for character in value):
        raise ValidationError("purpose is invalid")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    else:
        raise ValidationError(f"{label} must be timezone-aware")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _optional_aware_utc(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return _aware_utc(value, label)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _sorted_unique_segments(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be an array")
    parsed = tuple(_segment(item, f"{label} item") for item in value)
    if list(parsed) != sorted(parsed) or len(parsed) != len(set(parsed)):
        raise ValidationError(f"{label} must be sorted and contain no duplicates")
    return parsed


def _event_types(value: Any) -> tuple[str, ...]:
    parsed = _sorted_unique_segments(value, "allowed_event_types")
    if not parsed or not set(parsed).issubset(EVENT_TYPES):
        raise ValidationError("allowed_event_types contains an unsupported event type")
    return parsed


def _parse_activation(value: Any, *, owner: DestinationOwner) -> DestinationActivation:
    activation = _exact_mapping(
        value,
        "activation",
        {
            "enabled",
            "change_request_id",
            "reviewed_by",
            "reviewed_at",
            "review_expires_at",
        },
    )
    enabled = activation["enabled"]
    if type(enabled) is not bool:
        raise ValidationError("activation.enabled must be boolean")
    change_request_id = _optional_segment(
        activation["change_request_id"],
        "activation.change_request_id",
    )
    reviewed_by = _sorted_unique_segments(
        activation["reviewed_by"],
        "activation.reviewed_by",
    )
    reviewed_at = _optional_aware_utc(
        activation["reviewed_at"],
        "activation.reviewed_at",
    )
    review_expires_at = _optional_aware_utc(
        activation["review_expires_at"],
        "activation.review_expires_at",
    )

    evidence = (change_request_id, reviewed_at, review_expires_at)
    if not enabled:
        if any(item is not None for item in evidence) or reviewed_by:
            raise ValidationError(
                "disabled destination activation must not retain approval evidence"
            )
    else:
        if any(item is None for item in evidence) or not reviewed_by:
            raise ValidationError(
                "enabled destination activation requires complete review evidence"
            )
        assert reviewed_at is not None and review_expires_at is not None
        if review_expires_at <= reviewed_at:
            raise ValidationError("review_expires_at must follow reviewed_at")
        if review_expires_at - reviewed_at > timedelta(days=MAX_REVIEW_DAYS):
            raise ValidationError("destination review may not exceed 366 days")
        if reviewed_by == (owner.contact,):
            raise ValidationError(
                "destination owner contact may not be the sole activation reviewer"
            )
    return DestinationActivation(
        enabled=enabled,
        change_request_id=change_request_id,
        reviewed_by=reviewed_by,
        reviewed_at=reviewed_at,
        review_expires_at=review_expires_at,
    )


def _parse_destination(destination_id: str, value: Any) -> NotificationDestination:
    destination = _exact_mapping(
        value,
        f"destination {destination_id}",
        {
            "channel",
            "endpoint_env",
            "owner",
            "purpose",
            "recipient_scope",
            "data_classification",
            "allowed_event_types",
            "activation",
        },
    )
    channel = destination["channel"]
    if channel != CHANNEL:
        raise ValidationError("notification destination channel must be webhook")
    endpoint_env = destination["endpoint_env"]
    if not isinstance(endpoint_env, str) or not ENVIRONMENT_NAME.fullmatch(endpoint_env):
        raise ValidationError(
            "endpoint_env must name a local environment variable, not contain a URL"
        )
    owner_payload = _exact_mapping(
        destination["owner"],
        f"destination {destination_id} owner",
        {"team", "contact"},
    )
    owner = DestinationOwner(
        team=_segment(owner_payload["team"], "owner.team"),
        contact=_segment(owner_payload["contact"], "owner.contact"),
    )
    data_classification = destination["data_classification"]
    if data_classification != "internal":
        raise ValidationError("data_classification must be internal")
    return NotificationDestination(
        destination_id=_segment(destination_id, "destination_id"),
        channel=channel,
        endpoint_env=endpoint_env,
        owner=owner,
        purpose=_purpose(destination["purpose"]),
        recipient_scope=_segment(
            destination["recipient_scope"],
            "recipient_scope",
        ),
        data_classification=data_classification,
        allowed_event_types=_event_types(destination["allowed_event_types"]),
        activation=_parse_activation(destination["activation"], owner=owner),
    )


def load_notification_destinations(path: Path) -> dict[str, NotificationDestination]:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "notification destination configuration could not be loaded"
        ) from None
    root = _exact_mapping(
        payload,
        "notification destination configuration",
        {"model_version", "destinations"},
    )
    if root["model_version"] != MODEL_VERSION:
        raise ValidationError("notification destination model_version is unsupported")
    destinations = root["destinations"]
    if not isinstance(destinations, Mapping) or not destinations:
        raise ValidationError("destinations must be a non-empty mapping")
    if len(destinations) > MAX_DESTINATIONS:
        raise ValidationError("destination count exceeds the reviewed maximum")
    parsed = {
        destination_id: _parse_destination(destination_id, value)
        for destination_id, value in destinations.items()
    }
    if list(parsed) != sorted(parsed):
        raise ValidationError("destination IDs must be sorted")
    return parsed


def evaluate_destination_activation(
    destination: NotificationDestination,
    *,
    evaluated_at: datetime | str,
) -> dict[str, Any]:
    as_of = _aware_utc(evaluated_at, "evaluated_at")
    activation = destination.activation
    if not activation.enabled:
        status = "disabled"
    else:
        assert activation.reviewed_at is not None
        assert activation.review_expires_at is not None
        if as_of < activation.reviewed_at:
            status = "not_yet_reviewed"
        elif as_of >= activation.review_expires_at:
            status = "review_expired"
        else:
            status = "active"
    return {
        "destination_id": destination.destination_id,
        "model_version": MODEL_VERSION,
        "fingerprint": destination.fingerprint,
        "evaluated_at": as_of.isoformat(),
        "channel": destination.channel,
        "endpoint": {
            "environment_variable": destination.endpoint_env,
            "value_recorded": False,
        },
        "owner": {
            "team": destination.owner.team,
            "contact": destination.owner.contact,
        },
        "recipient_scope": destination.recipient_scope,
        "data_classification": destination.data_classification,
        "allowed_event_types": list(destination.allowed_event_types),
        "activation": {
            "enabled": activation.enabled,
            "status": status,
            "change_request_id": activation.change_request_id,
            "reviewed_by": list(activation.reviewed_by),
            "reviewed_at": _iso(activation.reviewed_at),
            "review_expires_at": _iso(activation.review_expires_at),
        },
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("destination contract summary must not be a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except (OSError, TypeError, ValueError):
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write destination contract summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate one reviewed portfolio-risk notification destination "
            "without resolving its endpoint or performing delivery."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_destinations.yaml"),
    )
    parser.add_argument("--destination-id", required=True)
    parser.add_argument("--evaluated-at", required=True)
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        destinations = load_notification_destinations(args.config)
        destination_id = _segment(args.destination_id, "destination_id")
        destination = destinations.get(destination_id)
        if destination is None:
            raise ValidationError("notification destination does not exist")
        summary = evaluate_destination_activation(
            destination,
            evaluated_at=args.evaluated_at,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError as exc:
        print(f"Notification destination rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError as exc:
        print(f"Notification destination validation failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Notification destination validation failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
