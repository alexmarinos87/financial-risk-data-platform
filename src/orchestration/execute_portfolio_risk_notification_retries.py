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

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    CHANNEL,
    DEFAULT_DESTINATION_CONFIG,
    DEFAULT_DESTINATION_ID,
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
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    DeliveryLockFactory,
    acquire_notification_delivery_lock,
)
from src.orchestration.portfolio_risk_notification_destination_authority import (
    resolve_notification_destination_authority,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
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
DestinationAuthorityResolver = Callable[..., dict[str, Any]]
DestinationAuthorityObserver = Callable[[Mapping[str, Any]], None]


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


def _destination_path(
    *,
    config_path: Path,
    destination_config_path: Path | None,
) -> Path:
    if destination_config_path is not None:
        return destination_config_path
    sibling = config_path.parent / "notification_destinations.yaml"
    return sibling if sibling.is_file() else DEFAULT_DESTINATION_CONFIG


def _event_types_for_retryable_events(
    *,
    retryable_event_ids: Sequence[str],
    event_by_id: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    event_types: list[str] = []
    for event_id in retryable_event_ids:
        event = event_by_id.get(event_id)
        if event is None:
            raise ValidationError(f"retained retry event evidence is missing {event_id}")
        event_type = safe_text(event.get("event_type"), "retry event_type")
        assert event_type is not None
        event_types.append(event_type)
    return sorted(set(event_types))


def _validate_destination_authority(
    value: Any,
    *,
    destination_id: str,
    endpoint_environment_variable: str,
    evaluated_at: datetime,
    event_types: Sequence[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError("destination authority resolver returned invalid evidence")
    required = {
        "authority_id",
        "destination_fingerprint",
        "destination_id",
        "endpoint_environment_variable",
        "evaluated_at",
        "evaluated_event_types",
        "model_version",
        "channel",
        "activation",
        "allowed_event_types",
        "active",
        "endpoint_value_recorded",
        "external_request_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
    }
    if set(value) != required:
        raise ValidationError("destination authority fields are invalid")
    if value["destination_id"] != destination_id:
        raise ValidationError("destination authority destination identity changed")
    if value["endpoint_environment_variable"] != endpoint_environment_variable:
        raise ValidationError("destination authority endpoint identity changed")
    authority_time = aware_utc(value["evaluated_at"], "destination evaluated_at")
    if authority_time != evaluated_at:
        raise ValidationError("destination authority evaluation time changed")
    if value["evaluated_event_types"] != sorted(set(event_types)):
        raise ValidationError("destination authority event evidence changed")
    if value["channel"] != CHANNEL or value["active"] is not True:
        raise ValidationError("destination authority is not active webhook authority")
    for key in (
        "endpoint_value_recorded",
        "external_request_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
    ):
        if value[key] is not False:
            raise ValidationError("destination authority side-effect evidence is invalid")
    for key in ("authority_id", "destination_fingerprint"):
        safe_text(value[key], f"destination authority {key}")
    return dict(value)


def execute_portfolio_risk_notification_retries(
    *,
    plan_path: Path,
    confirm_plan_id: str,
    request_id: str,
    config_path: Path,
    dsn: str,
    execute: bool = False,
    destination_config_path: Path | None = None,
    destination_id: str = DEFAULT_DESTINATION_ID,
    environment: Mapping[str, str] | None = None,
    reader: CandidateReader | None = None,
    attempt_writer: AttemptWriter | None = None,
    transport: Transport | None = None,
    clock: Clock | None = None,
    lock_factory: DeliveryLockFactory | None = None,
    destination_authority_resolver: DestinationAuthorityResolver | None = None,
    destination_authority_observer: DestinationAuthorityObserver | None = None,
) -> dict[str, Any]:
    if execute is not True:
        raise ValidationError("explicit --execute is required for manual retry delivery")
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    execution_time = _clock_utc(selected_clock, "execution_started_at")
    selected_request_id = safe_segment(request_id, "request_id")
    assert selected_request_id is not None
    selected_destination_id = safe_segment(destination_id, "destination_id")
    assert selected_destination_id is not None
    selected_destination_path = _destination_path(
        config_path=config_path,
        destination_config_path=destination_config_path,
    )
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
        raise ValidationError("execution start must not precede retry plan creation")
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
    selected_writer = attempt_writer or (
        lambda attempt: write_delivery_attempt(attempt, dsn=dsn)
    )
    selected_transport = transport or _default_transport
    selected_lock_factory = lock_factory or acquire_notification_delivery_lock
    selected_authority_resolver = (
        destination_authority_resolver
        or resolve_notification_destination_authority
    )
    filters = cast(Mapping[str, Any], retained_plan["filters"])

    with selected_lock_factory(dsn=dsn) as lock_evidence:
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
        event_types = _event_types_for_retryable_events(
            retryable_event_ids=retryable_event_ids,
            event_by_id=candidate_by_id,
        )
        authority_evaluated_at = _clock_utc(
            selected_clock,
            "destination_authority_evaluated_at",
        )
        if authority_evaluated_at < execution_time:
            raise ValidationError(
                "destination authority evaluation must not precede execution start"
            )
        destination_authority = _validate_destination_authority(
            selected_authority_resolver(
                destination_config_path=selected_destination_path,
                destination_id=selected_destination_id,
                delivery_endpoint_env=delivery_config.endpoint_env,
                evaluated_at=authority_evaluated_at,
                event_types=event_types,
                require_active=True,
            ),
            destination_id=selected_destination_id,
            endpoint_environment_variable=delivery_config.endpoint_env,
            evaluated_at=authority_evaluated_at,
            event_types=event_types,
        )
        if destination_authority_observer is not None:
            destination_authority_observer(destination_authority)

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
            "delivery_lock_key_fingerprint": lock_evidence["key_fingerprint"],
            "delivery_lock_model_version": lock_evidence["model_version"],
            "destination_authority_id": destination_authority["authority_id"],
            "destination_fingerprint": destination_authority[
                "destination_fingerprint"
            ],
            "destination_id": destination_authority["destination_id"],
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

        outcomes: list[dict[str, Any]] = []
        for expected in expected_attempts:
            event_id = cast(str, expected["event_id"])
            candidate = candidate_by_id.get(event_id)
            if candidate is None:
                raise ValidationError(
                    f"current candidate evidence is missing {event_id}"
                )
            payload = _canonical_payload(candidate)
            payload_sha256 = hashlib.sha256(payload).hexdigest()
            attempted_at = _clock_utc(selected_clock, "attempted_at")
            if attempted_at < execution_time:
                raise ValidationError("attempted_at must not precede execution start")
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
                    raise ValidationError(
                        "webhook transport returned an invalid HTTP status"
                    )
                http_status = response_status
                if 200 <= response_status <= 299:
                    outcome = "succeeded"
                else:
                    error_code = f"http_{response_status}"
            except DeliveryTransportError as exc:
                bounded_code = str(exc)
                error_code = (
                    bounded_code
                    if bounded_code in retry_policy.retryable_error_codes
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
        summary = {
            "execution_id": execution_id,
            "model_version": EXECUTION_MODEL_VERSION,
            "request_id": selected_request_id,
            "plan_id": retained_plan["plan_id"],
            "executed_at": execution_time.isoformat(),
            "channel": CHANNEL,
            "destination_authority": destination_authority,
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
            "concurrency_control": {
                "performed": True,
                "acquired": True,
                "released": False,
                "held_through_revalidation": True,
                "held_through_attempt_persistence": True,
                "model_version": lock_evidence["model_version"],
                "scope": lock_evidence["scope"],
                "key_fingerprint": lock_evidence["key_fingerprint"],
            },
            "response_bodies_recorded": False,
            "plan_mutated": False,
            "acknowledgement_mutated": False,
            "dead_letter_mutated": False,
        }

    summary["concurrency_control"]["released"] = True
    return summary


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
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--destination-config",
        type=Path,
        default=DEFAULT_DESTINATION_CONFIG,
    )
    parser.add_argument("--destination-id", default=DEFAULT_DESTINATION_ID)
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
            config_path=args.config,
            destination_config_path=args.destination_config,
            destination_id=args.destination_id,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except OverlapError:
        print(
            "Manual notification retry rejected: another notification delivery "
            "execution is already active",
            file=sys.stderr,
        )
        return 1
    except ValidationError as exc:
        print(f"Manual notification retry rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError:
        print(
            "Manual notification retry failed: PostgreSQL, lock, or attempt "
            "persistence failed; remote receivers must deduplicate by "
            "Idempotency-Key",
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
