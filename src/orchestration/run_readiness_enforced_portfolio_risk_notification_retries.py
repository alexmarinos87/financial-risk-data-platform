from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    DEFAULT_DESTINATION_CONFIG,
    DEFAULT_DESTINATION_ID,
    AttemptWriter,
    Transport,
)
from src.orchestration.execute_portfolio_risk_notification_retries import (
    DestinationAuthorityObserver,
    DestinationAuthorityResolver,
    execute_portfolio_risk_notification_retries,
)
from src.orchestration.plan_portfolio_risk_notification_retries import CandidateReader
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    DeliveryLockFactory,
    acquire_notification_delivery_lock,
)
from src.orchestration.portfolio_risk_notification_retry_plan_contract import (
    load_retry_plan,
)
from src.warehouse.notification_execution_readiness_enforcement import (
    enforce_notification_execution_readiness,
    validate_notification_execution_readiness_enforcement,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "portfolio-risk-notification-retry-readiness-enforced-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")

Clock = Callable[[], datetime]
Executor = Callable[..., dict[str, Any]]
PlanLoader = Callable[[Path], Mapping[str, Any]]
ReadinessEnforcer = Callable[..., Mapping[str, Any]]
ReadinessValidator = Callable[[Mapping[str, Any]], dict[str, Any]]


def _safe_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_TEXT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValidationError(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _destination_path(
    *,
    config_path: Path,
    destination_config_path: Path | None,
) -> Path:
    if destination_config_path is not None:
        return destination_config_path
    sibling = config_path.parent / "notification_destinations.yaml"
    return sibling if sibling.is_file() else DEFAULT_DESTINATION_CONFIG


def _lock_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError("notification delivery lock evidence must be a mapping")
    expected = {"acquired", "key_fingerprint", "model_version", "scope"}
    if set(value) != expected:
        raise ValidationError("notification delivery lock evidence fields are invalid")
    if value["acquired"] is not True:
        raise ValidationError("notification retry readiness requires an acquired lock")
    return {
        "acquired": True,
        "key_fingerprint": _safe_text(
            value["key_fingerprint"],
            "notification delivery lock key_fingerprint",
        ),
        "model_version": _safe_text(
            value["model_version"],
            "notification delivery lock model_version",
        ),
        "scope": _safe_text(value["scope"], "notification delivery lock scope"),
    }


def _validate_executor_summary(
    value: Any,
    *,
    plan_id: str,
    request_id: str,
    destination_id: str,
    lock: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise StorageError("notification retry executor returned invalid evidence")
    summary = dict(value)
    if summary.get("plan_id") != plan_id:
        raise ValidationError("notification retry execution plan identity changed")
    if summary.get("request_id") != request_id:
        raise ValidationError("notification retry execution request identity changed")
    authority = summary.get("destination_authority")
    if not isinstance(authority, Mapping):
        raise ValidationError("notification retry destination authority is unavailable")
    if authority.get("destination_id") != destination_id:
        raise ValidationError("notification retry destination identity changed")
    concurrency = summary.get("concurrency_control")
    if not isinstance(concurrency, Mapping):
        raise ValidationError("notification retry lock evidence is unavailable")
    for key in ("model_version", "scope", "key_fingerprint"):
        if concurrency.get(key) != lock[key]:
            raise ValidationError("notification retry lock identity changed")
    if concurrency.get("performed") is not True:
        raise ValidationError("notification retry did not use concurrency control")
    if concurrency.get("acquired") is not True:
        raise ValidationError("notification retry did not observe the acquired lock")
    if concurrency.get("released") is not True:
        raise ValidationError("notification retry did not close its held-lock context")
    return summary


def execute_readiness_enforced_portfolio_risk_notification_retries(
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
    executor: Executor | None = None,
    plan_loader: PlanLoader | None = None,
    readiness_enforcer: ReadinessEnforcer | None = None,
    readiness_validator: ReadinessValidator | None = None,
) -> dict[str, Any]:
    if execute is not True:
        raise ValidationError(
            "explicit --execute is required for readiness-enforced retries"
        )
    selected_plan_loader = plan_loader or load_retry_plan
    retained_plan = selected_plan_loader(plan_path)
    plan_id = _safe_text(retained_plan.get("plan_id"), "retry plan_id")
    if _safe_text(confirm_plan_id, "confirm_plan_id") != plan_id:
        raise ValidationError("confirm_plan_id does not match the retained retry plan")
    selected_request_id = _safe_text(request_id, "request_id")
    selected_destination_id = _safe_text(destination_id, "destination_id")
    selected_destination_path = _destination_path(
        config_path=config_path,
        destination_config_path=destination_config_path,
    )
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    enforced_at = _aware_utc(selected_clock(), "readiness_enforced_at")
    selected_lock_factory = lock_factory or acquire_notification_delivery_lock
    selected_enforcer = readiness_enforcer or enforce_notification_execution_readiness
    selected_validator = (
        readiness_validator or validate_notification_execution_readiness_enforcement
    )
    selected_executor = executor or execute_portfolio_risk_notification_retries
    held_lock_entries = 0
    summary: dict[str, Any]

    with selected_lock_factory(dsn=dsn) as raw_lock:
        lock = _lock_evidence(raw_lock)
        readiness = selected_validator(
            selected_enforcer(
                dsn=dsn,
                destination_id=selected_destination_id,
                execution_kind="retry",
                evaluated_at=enforced_at,
                delivery_config_path=config_path,
                destination_config_path=selected_destination_path,
                lock_evidence=lock,
            )
        )
        if readiness.get("execution_kind") != "retry":
            raise ValidationError("retry execution requires retry readiness authority")
        if readiness.get("destination_id") != selected_destination_id:
            raise ValidationError("retry readiness authority belongs to another destination")

        outer_dsn = dsn

        @contextmanager
        def reuse_held_lock(*, dsn: str) -> Iterator[Mapping[str, Any]]:
            nonlocal held_lock_entries
            if dsn != outer_dsn:
                raise ValidationError("retry executor changed the PostgreSQL DSN")
            if held_lock_entries != 0:
                raise ValidationError("retry executor attempted to reuse the lock twice")
            held_lock_entries += 1
            yield dict(lock)

        raw_summary = selected_executor(
            plan_path=plan_path,
            confirm_plan_id=confirm_plan_id,
            request_id=selected_request_id,
            config_path=config_path,
            dsn=dsn,
            execute=True,
            destination_config_path=selected_destination_path,
            destination_id=selected_destination_id,
            environment=environment,
            reader=reader,
            attempt_writer=attempt_writer,
            transport=transport,
            clock=selected_clock,
            lock_factory=reuse_held_lock,
            destination_authority_resolver=destination_authority_resolver,
            destination_authority_observer=destination_authority_observer,
        )
        if held_lock_entries != 1:
            raise ValidationError(
                "retry executor did not execute beneath the shared delivery lock"
            )
        summary = _validate_executor_summary(
            raw_summary,
            plan_id=plan_id,
            request_id=selected_request_id,
            destination_id=selected_destination_id,
            lock=lock,
        )
        summary["execution_readiness"] = readiness
        summary["governed_execution"] = {
            "model_version": MODEL_VERSION,
            "plan_id": plan_id,
            "destination_id": selected_destination_id,
            "readiness_enforcement_id": readiness["enforcement_id"],
            "single_physical_lock_acquisition": True,
            "nested_lock_reacquisition_performed": False,
            "lock_reused_by_retry_executor": True,
            "outer_lock_released": False,
        }

    governed = summary["governed_execution"]
    if not isinstance(governed, dict):
        raise StorageError("governed retry summary is invalid")
    governed["outer_lock_released"] = True
    return summary


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("readiness-enforced retry summary must not be a symbolic link")
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
        raise StorageError(
            "unable to write readiness-enforced retry execution summary"
        ) from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one exact notification retry plan only after current retained "
            "and freshly evaluated readiness both allow execution."
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
        summary = execute_readiness_enforced_portfolio_risk_notification_retries(
            plan_path=args.plan,
            confirm_plan_id=args.confirm_plan_id,
            request_id=args.request_id,
            config_path=args.config,
            dsn=args.dsn,
            execute=args.execute,
            destination_config_path=args.destination_config,
            destination_id=args.destination_id,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except OverlapError:
        print(
            "Readiness-enforced retry rejected: another notification delivery "
            "execution is already active",
            file=sys.stderr,
        )
        return 1
    except ValidationError as exc:
        print(f"Readiness-enforced retry rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Readiness-enforced retry failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Readiness-enforced retry failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
