"""Inspect retained worker readiness without granting execution permission."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from typing import Any, NoReturn

from src.common.exceptions import ValidationError
from src.warehouse.notification_worker_readiness_source import (
    source_bytes, source_identifier, source_time,
)

MODEL_VERSION = "portfolio-risk-worker-readiness-inspection-v1"
NO_AUTHORITY_FLAGS = (
    "failure_history_verified", "runtime_permission_granted",
    "notification_delivery_performed", "scheduler_mutated",
)
KINDS = {"initial", "retry"}
BLOCKERS = {"worker_authority_not_active"} | {
    f"{reason}:{kind}" for kind in KINDS for reason in (
        "readiness_missing", "readiness_review_changed", "readiness_plan_mismatch",
        "readiness_not_allowed",
    )
}


def _read(*, dsn: str, worker_id: str) -> dict[str, Any]:
    # No database dependency is imported by help or the default no-read path.
    from src.warehouse.notification_worker_readiness_reader import read_current_worker_readiness
    return read_current_worker_readiness(dsn=dsn, worker_id=worker_id)


def _summary(result: Mapping[str, Any], *, worker_id: str) -> dict[str, Any]:
    """Project trusted reader output, rejecting contradictory display evidence.

    This is not an independent source validator. The reader verifies actual
    documents; a caller-fabricated summary cannot authenticate a producer.
    """
    value = json.loads(source_bytes(result))
    status = value.get("status")
    if (
        value.get("model_version") != "portfolio-risk-worker-readiness-read-v1"
        or value.get("worker_id") != worker_id
        or status not in ("authority_missing", "blocked", "ready_sources")
        or value.get("database_read_performed") is not True
        or value.get("single_statement_read_only") is not True
        or any(value.get(flag) is not False for flag in NO_AUTHORITY_FLAGS)
    ):
        raise ValidationError("readiness inspection reader result is inconsistent")
    report = {
        "status": status, "worker_id": worker_id,
        "observed_at": source_time(value["observed_at"]).isoformat(),
        "authority_sequence": value.get("authority_sequence"),
        "authority_transition_id": value.get("authority_transition_id"),
    }
    snapshot = value.get("snapshot")
    if status == "authority_missing":
        if any(item is not None for item in (
            snapshot, report["authority_sequence"], report["authority_transition_id"],
        )):
            raise ValidationError("missing authority has contradictory evidence")
        return {**report, "readiness": [], "missing_execution_kinds": [],
                "blocking_reasons": ["authority_missing"], "snapshot_id": None}
    sequence = report["authority_sequence"]
    source_identifier(report["authority_transition_id"])
    if type(sequence) is not int or sequence < 1 or not isinstance(snapshot, dict):
        raise ValidationError("readiness inspection authority reference is invalid")
    if (
        snapshot.get("model_version") != "portfolio-risk-worker-readiness-snapshot-v1"
        or snapshot.get("worker_id") != worker_id
        or snapshot.get("authority_transition_id") != report["authority_transition_id"]
        or snapshot.get("observed_at") != report["observed_at"]
        or snapshot.get("outcome") != status
        or snapshot.get("runtime_permission_granted") is not False
        or snapshot.get("failure_history_verified") is not False
        or snapshot.get("current_authority_verified") is not False
    ):
        raise ValidationError("readiness inspection snapshot is inconsistent")
    snapshot_id = source_identifier(snapshot["snapshot_id"])
    rows, missing, reasons = (
        snapshot.get("readiness"), snapshot.get("missing_execution_kinds"),
        snapshot.get("blocking_reasons"),
    )
    if not isinstance(rows, list) or len(rows) > 2:
        raise ValidationError("readiness inspection rows exceed scope")
    if not isinstance(missing, list) or len(missing) > 2 or any(kind not in KINDS for kind in missing):
        raise ValidationError("readiness inspection missing kinds are invalid")
    if not isinstance(reasons, list) or len(reasons) > len(BLOCKERS) or any(reason not in BLOCKERS for reason in reasons):
        raise ValidationError("readiness inspection blocking reasons are invalid")
    if reasons != sorted(set(reasons)):
        raise ValidationError("readiness inspection blocking reasons are not canonical")
    projected = []
    kinds = list(missing)
    for row in rows:
        if not isinstance(row, dict) or row.get("execution_kind") not in KINDS:
            raise ValidationError("readiness inspection execution kind is invalid")
        kind, state = row["execution_kind"], row.get("status")
        if state not in ("allowed", "blocked", "stale", "superseded"):
            raise ValidationError("readiness inspection source status is invalid")
        digest = row.get("document_sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValidationError("readiness inspection source digest is invalid")
        kinds.append(kind)
        projected.append({
            "execution_kind": kind, "status": state,
            "record_id": source_identifier(row["record_id"]), "document_sha256": digest,
        })
    if not kinds or len(kinds) != len(set(kinds)):
        raise ValidationError("readiness inspection kind inventory is inconsistent")
    if status == "ready_sources" and (
        missing or reasons or any(row["status"] != "allowed" for row in projected)
    ):
        raise ValidationError("readiness inspection cannot promote blocked evidence")
    if status == "blocked" and not reasons:
        raise ValidationError("blocked readiness inspection requires a reason")
    return {**report, "snapshot_id": snapshot_id, "readiness": projected,
            "missing_execution_kinds": missing, "blocking_reasons": reasons}


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        # Unknown arguments may accidentally contain a DSN; do not echo values.
        self.print_usage(sys.stderr)
        self.exit(2, "Invalid readiness inspection arguments\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _Parser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--read-database", action="store_true")
    parser.add_argument("--dsn-env", default="WAREHOUSE_POSTGRES_DSN")
    args = parser.parse_args(argv)
    attempted = False
    try:
        worker_id = source_identifier(args.worker_id)
        if re.fullmatch(r"[A-Z][A-Z0-9_]{2,127}", args.dsn_env) is None:
            raise ValidationError("DSN environment variable name is invalid")
        result: dict[str, Any] = {"status": "not_requested", "worker_id": worker_id}
        if args.read_database:
            dsn = os.environ.get(args.dsn_env)
            if not dsn or not dsn.strip():
                raise ValidationError("DSN environment variable is not configured")
            attempted = True
            result = _summary(_read(dsn=dsn, worker_id=worker_id), worker_id=worker_id)
        report = {**result, "model_version": MODEL_VERSION,
                  "database_read_attempted": attempted, "database_read_completed": attempted,
                  **dict.fromkeys(NO_AUTHORITY_FLAGS, False)}
        print(source_bytes(report).decode("ascii"))
        return 2 if result["status"] in ("blocked", "authority_missing") else 0
    except Exception:
        # A failed read may have reached PostgreSQL; do not assert that no I/O ran.
        print(json.dumps({"model_version": MODEL_VERSION, "status": "failed",
                          "database_read_attempted": attempted, "database_read_completed": False,
                          **dict.fromkeys(NO_AUTHORITY_FLAGS, False)}, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
