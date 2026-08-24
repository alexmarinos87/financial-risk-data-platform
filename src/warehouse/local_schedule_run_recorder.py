from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.local_schedule_run_contract import (
    build_local_schedule_run_id as build_local_schedule_run_id,
    canonical_local_schedule_run_bytes,
    read_local_schedule_run,
    validate_local_schedule_run,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _summary(run: Mapping[str, Any], *, created: bool, digest: str) -> dict[str, Any]:
    return {
        "run_id": run["run_id"],
        "request_id": run["request_id"],
        "plan_id": run["plan_id"],
        "authority_id": run["authority_id"],
        "authority_type": run["authority_type"],
        "run_status": run["run_status"],
        "selected_session_count": run["selected_session_count"],
        "started_session_count": run["started_session_count"],
        "completed_session_count": run["completed_session_count"],
        "checkpoint_after": run["checkpoint_after"],
        "document_sha256": digest,
        "created": created,
    }


def record_local_schedule_run(
    *,
    dsn: str,
    run: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_local_schedule_run(run)
    canonical = canonical_local_schedule_run_bytes(validated)
    digest = hashlib.sha256(canonical).hexdigest()
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Local schedule run recording requires psycopg") from exc

    created = False
    stored: dict[str, Any] | None = None
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_json, document_sha256
                    FROM risk_platform.local_schedule_runs
                    WHERE request_id = %s
                    """,
                    (validated["request_id"],),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    existing_json, existing_digest = existing
                    if existing_digest != digest or existing_json != validated:
                        raise ValidationError(
                            "request_id already exists with different run evidence"
                        )
                    return _summary(validated, created=False, digest=digest)

                cursor.execute(
                    """
                    INSERT INTO risk_platform.local_schedule_runs (
                        run_id,
                        model_version,
                        request_id,
                        plan_id,
                        authority_id,
                        authority_type,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        calendar_fingerprint,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_id,
                        mandate_fingerprint,
                        as_of_date,
                        latest_expected_session,
                        readiness_decision_id,
                        readiness_document_sha256,
                        override_id,
                        authorized_at,
                        started_at,
                        finished_at,
                        run_status,
                        checkpoint_before,
                        checkpoint_after,
                        selected_session_count,
                        started_session_count,
                        completed_session_count,
                        failed_session,
                        failed_stage_index,
                        failed_stage_name,
                        failure_code,
                        run_json,
                        document_sha256
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING run_id
                    """,
                    (
                        validated["run_id"],
                        validated["model_version"],
                        validated["request_id"],
                        validated["plan_id"],
                        validated["authority_id"],
                        validated["authority_type"],
                        validated["schedule_id"],
                        validated["schedule_fingerprint"],
                        validated["calendar_id"],
                        validated["calendar_fingerprint"],
                        validated["portfolio_id"],
                        validated["risk_limit_policy_id"],
                        validated["mandate_id"],
                        validated["mandate_fingerprint"],
                        validated["as_of_date"],
                        validated["latest_expected_session"],
                        validated["readiness_decision_id"],
                        validated["readiness_document_sha256"],
                        validated["override_id"],
                        validated["authorized_at"],
                        validated["started_at"],
                        validated["finished_at"],
                        validated["run_status"],
                        validated["checkpoint_before"],
                        validated["checkpoint_after"],
                        validated["selected_session_count"],
                        validated["started_session_count"],
                        validated["completed_session_count"],
                        validated["failed_session"],
                        validated["failed_stage_index"],
                        validated["failed_stage_name"],
                        validated["failure_code"],
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    """
                    SELECT run_json, document_sha256
                    FROM risk_platform.local_schedule_runs
                    WHERE run_id = %s
                    """,
                    (validated["run_id"],),
                )
                row = cursor.fetchone()
                if row is None:
                    raise StorageError("local schedule run could not be read after insert")
                stored_json, stored_digest = row
                if stored_digest != digest or stored_json != validated:
                    raise ValidationError(
                        "run_id already exists with different run evidence"
                    )
                stored = dict(stored_json)
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("local schedule run database operation failed") from None

    if stored is None:  # pragma: no cover - guarded by transaction above.
        raise StorageError("local schedule run result is unavailable")
    return _summary(stored, created=created, digest=digest)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append one terminal local schedule run document to PostgreSQL."
    )
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = record_local_schedule_run(
            dsn=args.dsn,
            run=read_local_schedule_run(args.run),
        )
    except ValidationError as exc:
        print(f"Local schedule run rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Local schedule run recording failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
