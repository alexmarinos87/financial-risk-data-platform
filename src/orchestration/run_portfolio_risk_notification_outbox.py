from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_risk_notification_outbox import (
    MAX_NOTIFICATION_EVENTS,
    PortfolioRiskNotificationOutput,
    build_portfolio_risk_notification_outbox,
)
from ..common.exceptions import StorageError, ValidationError
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config, validate_storage_config
from ..warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

INPUT_VIEW = "portfolio_risk_limit_actionable_transitions"
OUTPUT_DATASET = "portfolio_risk_notification_outbox"

Reader = Callable[..., list[dict[str, Any]]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]

TRANSITION_COLUMNS = (
    "calculation_id",
    "model_version",
    "policy_id",
    "policy_fingerprint",
    "portfolio_id",
    "base_currency",
    "definition_fingerprint",
    "attribution_calculation_id",
    "attribution_model_version",
    "weighting_method",
    "covariance_method",
    "correlation_method",
    "covariance_window",
    "annualization_days",
    "ts_event",
    "ts_ingest",
    "metric_name",
    "subject_type",
    "subject_key",
    "unit",
    "observed_value",
    "observed_signed_value",
    "warning_threshold",
    "critical_threshold",
    "status",
    "is_breach",
    "breach_threshold",
    "breach_excess",
    "previous_status",
    "previous_calculation_id",
    "previous_subject_key",
    "transition_type",
    "severity_rank",
    "subject_changed",
)


def _calendar_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        ) from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _max_events(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "max_events must be a positive integer"
        ) from exc
    if not 1 <= parsed <= MAX_NOTIFICATION_EVENTS:
        raise argparse.ArgumentTypeError(
            f"max_events must be between 1 and {MAX_NOTIFICATION_EVENTS}"
        )
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build replay-safe notification-outbox candidates from current "
            "portfolio risk-limit transitions without delivering messages."
        )
    )
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--max-events",
        type=_max_events,
        default=MAX_NOTIFICATION_EVENTS,
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "WAREHOUSE_POSTGRES_DSN",
            DEFAULT_POSTGRES_DSN,
        ),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _require_output_dataset(storage_config: dict[str, Any]) -> None:
    validate_storage_config(storage_config)
    datasets = storage_config["storage"]["curated"]["datasets"]
    if OUTPUT_DATASET not in datasets:
        raise StorageError(
            "Storage configuration is missing "
            f"'{OUTPUT_DATASET}'"
        )


def read_actionable_transitions(
    *,
    dsn: str,
    policy_id: str,
    start_date: date | None,
    end_date: date,
    max_events: int,
    schema_name: str = "risk_platform",
) -> list[dict[str, Any]]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise StorageError("PostgreSQL DSN must be non-empty text")
    if not isinstance(policy_id, str) or not policy_id.strip():
        raise ValidationError("policy_id must be non-empty text")
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    if (
        type(max_events) is not int
        or not 1 <= max_events <= MAX_NOTIFICATION_EVENTS
    ):
        raise ValidationError(
            f"max_events must be between 1 and {MAX_NOTIFICATION_EVENTS}"
        )

    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL transition reading requires psycopg. "
            "Run `make setup` first."
        ) from exc

    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    quoted_view = '"' + INPUT_VIEW.replace('"', '""') + '"'
    selected = ", ".join('"' + column + '"' for column in TRANSITION_COLUMNS)
    clauses = [
        "policy_id = %s",
        "ts_event < (%s::date + INTERVAL '1 day')",
    ]
    parameters: list[Any] = [policy_id.strip(), end_date]
    if start_date is not None:
        clauses.append("ts_event >= %s::date")
        parameters.append(start_date)
    parameters.append(max_events + 1)
    statement = (
        f"SELECT {selected} FROM {quoted_schema}.{quoted_view} "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY ts_event, metric_name, transition_type, calculation_id "
        "LIMIT %s"
    )

    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                records = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "Unable to read current portfolio risk-limit transitions"
        ) from None

    if len(records) > max_events:
        raise ValidationError(
            "notification outbox input exceeds max_events; split the date range"
        )
    if not records:
        raise ValidationError(
            "no actionable risk-limit transitions matched the requested range"
        )
    return records


def _publish(
    events: tuple[dict[str, Any], ...],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    written = 0
    for event in events:
        try:
            result = writer(
                [event],
                kind="curated",
                dataset=OUTPUT_DATASET,
                storage_config=storage_config,
            )
        except Exception:
            raise StorageError(
                "Portfolio notification-outbox publication failed; rerun is safe"
            ) from None
        if type(result) is not int or result not in {0, 1}:
            raise StorageError(
                "Portfolio notification-outbox publication returned an invalid result"
            )
        written += result
    return written


def run_portfolio_risk_notification_outbox(
    *,
    policy_id: str,
    start_date: date | None,
    end_date: date,
    max_events: int,
    dsn: str,
    storage_config_path: Path,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
) -> dict[str, Any]:
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    selected_storage_loader = storage_config_loader or load_storage_config
    try:
        storage_config = selected_storage_loader(storage_config_path)
    except Exception:
        raise StorageError("Storage configuration is invalid") from None
    if not isinstance(storage_config, dict):
        raise StorageError("Storage configuration is invalid")
    _require_output_dataset(storage_config)

    selected_reader = reader or read_actionable_transitions
    try:
        records = selected_reader(
            dsn=dsn,
            policy_id=policy_id,
            start_date=start_date,
            end_date=end_date,
            max_events=max_events,
        )
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "Unable to read current portfolio risk-limit transitions"
        ) from None

    output: PortfolioRiskNotificationOutput = (
        build_portfolio_risk_notification_outbox(
            records,
            start_date=start_date,
            end_date=end_date,
            max_events=max_events,
        )
    )
    selected_writer = writer or write_records
    written = _publish(
        output.events,
        storage_config=storage_config,
        writer=selected_writer,
    )
    selected = len(output.events)
    pending = sum(
        1
        for event in output.events
        if event["delivery_disposition"] == "pending"
    )
    suppressed = selected - pending
    latest = output.events[-1]

    return {
        "run_id": str(uuid4()),
        "policy_id": policy_id,
        "selection": dict(output.diagnostics),
        "parameters": {
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
            "max_events": max_events,
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": selected,
                "records_written": written,
                "records_already_present": selected - written,
                "pending_delivery_candidates": pending,
                "suppressed_candidates": suppressed,
            }
        },
        "delivery": {
            "performed": False,
            "external_destinations": 0,
            "reason": "delivery_not_implemented",
        },
        "latest_event": {
            "event_id": latest["event_id"],
            "event_type": latest["event_type"],
            "transition_type": latest["transition_type"],
            "delivery_disposition": latest["delivery_disposition"],
            "metric_name": latest["metric_name"],
            "subject_key": latest["subject_key"],
            "ts_event": latest["ts_event"].isoformat(),
        },
    }


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise StorageError(
            "Unable to write portfolio notification-outbox summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_portfolio_risk_notification_outbox(
            policy_id=args.policy_id,
            start_date=args.start_date,
            end_date=args.end_date,
            max_events=args.max_events,
            dsn=args.dsn,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Portfolio notification-outbox generation failed: input data "
            "or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Portfolio notification-outbox generation failed: local storage "
            "or PostgreSQL operation failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio notification-outbox generation failed: unexpected "
            "local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
