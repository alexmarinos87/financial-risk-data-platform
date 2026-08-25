from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    CHANNEL,
    MODEL_VERSION as DELIVERY_MODEL_VERSION,
    AttemptWriter,
    DeliveryTransportError,
    Transport,
    _attempt_id,
    _canonical_payload,
    _default_transport,
    _endpoint,
    write_delivery_attempt,
)
from src.orchestration.plan_portfolio_risk_notification_retries import (
    CandidateReader,
    plan_portfolio_risk_notification_retries,
    read_notification_retry_candidates,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    ERROR_CODE_PATTERN,
    EXECUTION_MODEL_VERSION,
    aware_utc,
    load_retry_execution_contract,
    safe_segment,
    safe_text,
)
from src.orchestration.portfolio_risk_notification_retry_plan_contract import (
    assert_retry_plan_is_current,
    load_retry_plan,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

Clock = Callable[[], datetime]


def _execution_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{EXECUTION_MODEL_VERSION}-execution-{digest}"


def _clock_utc(clock: Clock, label: str) -> datetime:
    return aware_utc(clock(), label)


def execute_portfolio_risk_notification_retries(
    *,
    plan_path: Path,
    confirm_plan_id: str,
    request_id: str,
    executed_at: datetime | str,
    config_path: Path,
    dsn: str,
    execute: bool = False,
    environment: Mapping[str, str] | None = None,
    reader: CandidateReader | None = None,
    attempt_writer: AttemptWriter | None = None,
    transport: Transport | None = None,
    clock: Clock | None = None,
) -> dict[str, Any]:
    if execute is not True:
        raise ValidationError("explicit --execute is required for manual retry delivery")
    execution_time = aware_utc(executed_at, "executed_at")
    selected_request_id = safe_segment(request_id, "request_id")
    assert selected_request_id is not None
    retained_plan = load_retry_plan(plan_path)
    confirmed = safe_text(confirm_plan_id, "confirm_plan_id")
    if confirmed != retained_plan["plan_id"]:
        raise ValidationError("confirm_plan_id does not match the retained retry plan")

    delivery_config, retry_policy, execution_policy = load_retry_execution_contract(
        config_path
    )
    if not execution_policy.enabled:
        raise ValidationError("manual retry execution is disabled in reviewed configuration")
    if not delivery_config.enabled:
        raise ValidationError("webhook delivery is disabled in reviewed configuration")
    retained_delivery = cast(Mapping[str, Any], retained_plan["delivery_config"])
    retained_retry_policy = cast(Mapping[str, Any], retained_plan["retry_policy"])
    if retained_delivery["enabled"] is not True:
        raise ValidationError("retained retry plan was not generated under enabled delivery")
    if retained_delivery["fingerprint"] != delivery_config.fingerprint:
        raise ValidationError("delivery configuration fingerprint changed after planning")
    if retained_retry_policy["fingerprint"] != retry_policy.fingerprint:
        raise ValidationError("retry policy fingerprint changed after planning")
    if (
        retained_delivery["max_attempts_per_event"]
        != delivery_config.max_attempts_per_event
    ):
        raise ValidationError("delivery attempt limit changed after planning")

    planned_at = aware_utc(retained_plan["planned_at"], "retry plan planned_at")
    plan_age_seconds = (execution_time - planned_at).total_seconds()
    if plan_age_seconds < 0:
        raise ValidationError("executed_at must not precede retry plan creation")
    if plan_age_seconds > execution_policy.max_plan_age_seconds:
        raise ValidationError("retry plan exceeds the manual execution age limit")
    retryable_event_ids = cast(list[str], retained_plan["retryable_event_ids"])
    if not retryable_event_ids:
        raise ValidationError("retry plan contains no retryable events")
    if len(retryable_event_ids) > execution_policy.max_events:
        raise ValidationError("retry plan exceeds the manual execution event limit")

    selected_environment = environment if environment is not None else os.environ
    raw_endpoint = selected_environment.get(delivery_config.endpoint_env)
    if raw_endpoint is None:
        raise ValidationError(
            f"webhook endpoint environment variable {delivery_config.endpoint_env} is not set"
        )
    endpoint, endpoint_host = _endpoint(raw_endpoint)

    selected_reader = reader or read_notification_retry_candidates
    filters = cast(Mapping[str, Any], retained_plan["filters"])
    candidates = selected_reader(
        dsn=dsn,
        planned_at=execution_time,
        max_candidate_rows=retry_policy.max_candidate_rows,
        policy_id=filters["policy_id"],
        portfolio_id=filters["portfolio_id"],
    )
    if not isinstance(candidates, list):
        raise StorageError("notification retry reader returned invalid evidence")
    if len(candidates) > retry_policy.max_candidate_rows:
        raise ValidationError("notification retry evidence exceeds max_candidate_rows")
    current_plan = plan_portfolio_risk_notification_retries(
        config_path=config_path,
        dsn=dsn,
        planned_at=execution_time,
        policy_id=cast(str | None, filters["policy_id"]),
        portfolio_id=cast(str | None, filters["portfolio_id"]),
        reader=lambda **_: candidates,
    )
    assert_retry_plan_is_current(retained_plan, current_plan)

    candidate_by_id = {
        cast(str, candidate.get("event_id")): candidate for candidate in candidates
    }
    retained_events = cast(list[Mapping[str, Any]], retained_plan["events"])
    event_by_id = {
        cast(str, event["event_id"]): event for event in retained_events
    }
    expected_attempts: list[dict[str, Any]] = []
    for event_id in retryable_event_ids:
        event = event_by_id[event_id]
        attempt_number = cast(int, event["attempt_count"]) + 1
        if attempt_number > delivery_config.max_attempts_per_event:
            raise ValidationError(
                f"event {event_id} has no remaining delivery attempt capacity"
            )
        expected_attempts.append(
            {
                "attempt_id": _attempt_id(event_id, attempt_number),
                "attempt_number": attempt_number,
                "event_document_sha256": event["event_document_sha256"],
                "event_id": event_id,
            }
        )
    identity = {
        "channel": CHANNEL,
        "delivery_config_fingerprint": delivery_config.fingerprint,
        "endpoint_host": endpoint_host,
        "executed_at": execution_time.isoformat(),
        "expected_attempts": expected_attempts,
        "model_version": EXECUTION_MODEL_VERSION,
        "plan_id": retained_plan["plan_id"],
        "request_id": selected_request_id,
        "retry_execution_policy_fingerprint": execution_policy.fingerprint,
        "retry_policy_fingerprint": retry_policy.fingerprint,
    }
    execution_id = _execution_id(identity)

    selected_writer = attempt_writer or (
        lambda attempt: write_delivery_attempt(attempt, dsn=dsn)
    )
    selected_transport = transport or _default_transport
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    outcomes: list[dict[str, Any]] = []
    for expected in expected_attempts:
        event_id = cast(str, expected["event_id"])
        candidate = candidate_by_id.get(event_id)
        if candidate is None:
            raise ValidationError(f"current candidate evidence is missing {event_id}")
        payload = _canonical_payload(candidate)
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        attempted_at = _clock_utc(selected_clock, "attempted_at")
        if attempted_at < execution_time:
            raise ValidationError("attempted_at must not precede executed_at")
        outcome = "failed"
        http_status: int | None = None
        error_code: str | None = None
        try:
            response_status = selected_transport(
                endpoint,
                payload,
                {
                    "Content-Type": "application/json",
                    "Idempotency-Key": event_id,
                    "User-Agent": "financial-risk-data-platform/1",
                },
                float(delivery_config.timeout_seconds),
            )
            if type(response_status) is not int or not 100 <= response_status <= 599:
                raise ValidationError("webhook transport returned an invalid HTTP status")
            http_status = response_status
            if 200 <= response_status <= 299:
                outcome = "succeeded"
            else:
                error_code = f"http_{response_status}"
        except DeliveryTransportError as exc:
            bounded_code = str(exc)
            error_code = (
                bounded_code
                if ERROR_CODE_PATTERN.fullmatch(bounded_code)
                else "network_error"
            )
        attempt = {
            "attempt_id": expected["attempt_id"],
            "model_version": DELIVERY_MODEL_VERSION,
            "event_id": event_id,
            "channel": CHANNEL,
            "attempt_number": expected["attempt_number"],
            "idempotency_key": event_id,
            "attempted_at": attempted_at,
            "outcome": outcome,
            "http_status": http_status,
            "error_code": error_code,
            "endpoint_host": endpoint_host,
            "payload_sha256": payload_sha256,
        }
        selected_writer(attempt)
        outcomes.append(
            {
                "attempt_id": attempt["attempt_id"],
                "attempt_number": attempt["attempt_number"],
                "attempted_at": attempted_at.isoformat(),
                "error_code": error_code,
                "event_id": event_id,
                "http_status": http_status,
                "outcome": outcome,
                "payload_sha256": payload_sha256,
            }
        )

    succeeded = sum(outcome["outcome"] == "succeeded" for outcome in outcomes)
    failed = len(outcomes) - succeeded
    return {
        "execution_id": execution_id,
        "model_version": EXECUTION_MODEL_VERSION,
        "request_id": selected_request_id,
        "plan_id": retained_plan["plan_id"],
        "executed_at": execution_time.isoformat(),
        "channel": CHANNEL,
        "endpoint": {
            "host": endpoint_host,
            "full_url_recorded": False,
        },
        "configuration": {
            "delivery_fingerprint": delivery_config.fingerprint,
            "retry_execution_policy_fingerprint": execution_policy.fingerprint,
            "retry_policy_fingerprint": retry_policy.fingerprint,
        },
        "revalidation": {
            "performed": True,
            "current_plan_id": current_plan["plan_id"],
            "events_checked": len(cast(list[Any], retained_plan["events"])),
            "exact_event_evidence_unchanged": True,
        },
        "selection": {
            "planned_retryable_events": len(retryable_event_ids),
            "executed_events": len(outcomes),
            "max_events": execution_policy.max_events,
        },
        "outcomes": outcomes,
        "outcome_counts": {
            "succeeded": succeeded,
            "failed": failed,
        },
        "execution": {
            "requested": True,
            "performed": True,
            "external_requests_performed": len(outcomes),
            "delivery_attempts_written": len(outcomes),
        },
        "response_bodies_recorded": False,
        "plan_mutated": False,
        "acknowledgement_mutated": False,
        "dead_letter_mutated": False,
    }


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("manual retry summary must not be a symbolic link")
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
        raise StorageError("unable to write manual retry execution summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one exact retained notification retry plan under an explicit "
            "disabled-by-default manual delivery gate."
        )
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--confirm-plan-id", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--executed-at", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = execute_portfolio_risk_notification_retries(
            plan_path=args.plan,
            confirm_plan_id=args.confirm_plan_id,
            request_id=args.request_id,
            executed_at=args.executed_at,
            config_path=args.config,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError as exc:
        print(f"Manual notification retry rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError:
        print(
            "Manual notification retry failed: PostgreSQL or attempt persistence "
            "failed; remote receivers must deduplicate by Idempotency-Key",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Manual notification retry failed: unexpected local failure", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())