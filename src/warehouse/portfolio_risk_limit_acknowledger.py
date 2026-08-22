from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import StorageError, ValidationError

DEFAULT_POSTGRES_DSN = (
    "postgresql://risk_user:risk_password@localhost:5433/risk_platform"
)
MODEL_VERSION = "portfolio-risk-limit-ack-v1"
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
DISPOSITIONS = frozenset({"investigating", "accepted", "false_positive"})
MAX_CALCULATION_ID_LENGTH = 256
MAX_ACTOR_LENGTH = 128
MAX_REASON_LENGTH = 2_000


def _bounded_text(value: Any, label: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip()
    if not parsed or len(parsed) > maximum:
        raise ValidationError(
            f"{label} must contain between 1 and {maximum} characters"
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in parsed):
        raise ValidationError(f"{label} must not contain control characters")
    return parsed


def _request_id(value: Any) -> str:
    parsed = _bounded_text(value, "request_id", 128)
    if REQUEST_ID_PATTERN.fullmatch(parsed) is None:
        raise ValidationError("request_id has an invalid format")
    return parsed


def _disposition(value: Any) -> str:
    parsed = _bounded_text(value, "disposition", 32).lower()
    if parsed not in DISPOSITIONS:
        raise ValidationError(
            "disposition must be investigating, accepted or false_positive"
        )
    return parsed


def _aware_utc(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError("acknowledged_at must be a timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def acknowledgement_id(evaluation_calculation_id: str, request_id: str) -> str:
    calculation_id = _bounded_text(
        evaluation_calculation_id,
        "evaluation_calculation_id",
        MAX_CALCULATION_ID_LENGTH,
    )
    canonical_request_id = _request_id(request_id)
    payload = {
        "evaluation_calculation_id": calculation_id,
        "model_version": MODEL_VERSION,
        "request_id": canonical_request_id,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-{digest}"


def acknowledge_current_breach(
    *,
    dsn: str,
    evaluation_calculation_id: str,
    request_id: str,
    acknowledged_by: str,
    reason: str,
    disposition: str,
    acknowledged_at: datetime | str | None = None,
) -> dict[str, Any]:
    calculation_id = _bounded_text(
        evaluation_calculation_id,
        "evaluation_calculation_id",
        MAX_CALCULATION_ID_LENGTH,
    )
    canonical_request_id = _request_id(request_id)
    actor = _bounded_text(acknowledged_by, "acknowledged_by", MAX_ACTOR_LENGTH)
    canonical_reason = _bounded_text(reason, "reason", MAX_REASON_LENGTH)
    canonical_disposition = _disposition(disposition)
    timestamp = _aware_utc(acknowledged_at)
    ack_id = acknowledgement_id(calculation_id, canonical_request_id)

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Risk-limit acknowledgement requires psycopg") from exc

    stored: tuple[Any, ...] | None = None
    created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT calculation_id, ts_event
                    FROM risk_platform.portfolio_risk_limit_breaches
                    WHERE calculation_id = %s
                    """,
                    (calculation_id,),
                )
                target = cursor.fetchone()
                if target is None:
                    raise ValidationError(
                        "evaluation_calculation_id must identify a current breach"
                    )
                if timestamp < target[1]:
                    raise ValidationError(
                        "acknowledged_at must be on or after the breach event"
                    )

                cursor.execute(
                    """
                    INSERT INTO risk_platform.portfolio_risk_limit_acknowledgements (
                        acknowledgement_id,
                        model_version,
                        evaluation_calculation_id,
                        request_id,
                        acknowledged_at,
                        acknowledged_by,
                        disposition,
                        reason
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (acknowledgement_id) DO NOTHING
                    RETURNING acknowledgement_id
                    """,
                    (
                        ack_id,
                        MODEL_VERSION,
                        calculation_id,
                        canonical_request_id,
                        timestamp,
                        actor,
                        canonical_disposition,
                        canonical_reason,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    """
                    SELECT
                        acknowledgement_id,
                        model_version,
                        evaluation_calculation_id,
                        request_id,
                        acknowledged_at,
                        acknowledged_by,
                        disposition,
                        reason
                    FROM risk_platform.portfolio_risk_limit_acknowledgements
                    WHERE acknowledgement_id = %s
                    """,
                    (ack_id,),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "Risk-limit acknowledgement could not be read after insert"
                    )
                immutable = (
                    ack_id,
                    MODEL_VERSION,
                    calculation_id,
                    canonical_request_id,
                    actor,
                    canonical_disposition,
                    canonical_reason,
                )
                stored_immutable = (
                    stored[0],
                    stored[1],
                    stored[2],
                    stored[3],
                    stored[5],
                    stored[6],
                    stored[7],
                )
                if stored_immutable != immutable:
                    raise ValidationError(
                        "request_id already exists with different acknowledgement content"
                    )
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("Risk-limit acknowledgement database operation failed") from None

    if stored is None:  # pragma: no cover - guarded by the transaction above.
        raise StorageError("Risk-limit acknowledgement result is unavailable")
    stored_timestamp = stored[4]
    if not isinstance(stored_timestamp, datetime):
        raise StorageError("Stored acknowledgement timestamp is incompatible")
    return {
        "acknowledgement_id": ack_id,
        "model_version": MODEL_VERSION,
        "evaluation_calculation_id": calculation_id,
        "request_id": canonical_request_id,
        "acknowledged_at": stored_timestamp.astimezone(timezone.utc).isoformat(),
        "acknowledged_by": actor,
        "disposition": canonical_disposition,
        "created": created,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Append an idempotent human acknowledgement for one current "
            "portfolio risk-limit breach."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument("--calculation-id", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--acknowledged-by", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument(
        "--disposition",
        required=True,
        choices=sorted(DISPOSITIONS),
    )
    parser.add_argument("--acknowledged-at")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        summary = acknowledge_current_breach(
            dsn=args.dsn,
            evaluation_calculation_id=args.calculation_id,
            request_id=args.request_id,
            acknowledged_by=args.acknowledged_by,
            reason=args.reason,
            disposition=args.disposition,
            acknowledged_at=args.acknowledged_at,
        )
    except ValidationError as exc:
        print(f"Risk-limit acknowledgement rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Risk-limit acknowledgement failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
