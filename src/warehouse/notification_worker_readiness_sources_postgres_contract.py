"""Disposable PostgreSQL proof helpers; no production recording entry point."""
from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.common.exceptions import StorageError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.orchestration.plan_notification_worker import build_notification_worker_plan, load_notification_workers
from src.orchestration.portfolio_risk_notification_destination_contract import NotificationDestination
from src.orchestration.portfolio_risk_notification_retry_execution_policy import load_retry_execution_contract
from src.warehouse.notification_execution_readiness_gate import evaluate_notification_execution_readiness
from src.warehouse.notification_execution_readiness_history_contract import (
    build_notification_execution_readiness_record, validate_notification_execution_readiness_record,
)
from src.warehouse.notification_worker_readiness_sources import MAX_SOURCE_BYTES, validate_worker_readiness_sources
from src.warehouse.notification_worker_readiness_sources_reader import (
    READINESS_SOURCES_SQL, read_worker_readiness_sources, read_worker_readiness_sources_with_cursor,
)

TABLE = "risk_platform.notification_execution_readiness_decisions"


def _fixture_inputs(*, destination: NotificationDestination, evidence: dict[str, Any],
                    now: datetime, prefix: str) -> dict[str, Any]:
    original = load_notification_workers(Path("config/notification_workers.yaml"))["risk-operations-managed"]
    worker = replace(original, enabled=True, destination_id=destination.destination_id)
    delivery, policy, execution = load_retry_execution_contract(Path("config/notification_delivery.yaml"))
    delivery = replace(delivery, enabled=True, endpoint_env=destination.endpoint_env)
    execution = replace(execution, enabled=True)

    def plan_for(*, max_age: int = 300, retry_enabled: bool = True,
                 initial_only: bool = False, missing: bool = False) -> dict[str, Any]:
        target = replace(destination, destination_id=destination.destination_id + "-missing") if missing else destination
        selected = replace(worker, destination_id=target.destination_id,
                           readiness=replace(worker.readiness, max_age_seconds=max_age),
                           execution_kinds=("initial",) if initial_only else ("initial", "retry"))
        return build_notification_worker_plan(
            worker=selected, delivery=delivery, retry_policy=policy,
            retry_execution=replace(execution, enabled=retry_enabled), destination=target, planned_at=now,
        )

    def record_for(kind: str, seconds: int, *, retry_enabled: bool = True) -> dict[str, Any]:
        instant = now - timedelta(seconds=seconds)
        decision = evaluate_notification_execution_readiness(
            execution_kind=kind, evaluated_at=instant, delivery_config=delivery,
            retry_policy_fingerprint=policy.fingerprint,
            retry_execution_policy=replace(execution, enabled=retry_enabled), destination=destination,
            activation_review=evidence["activation_review"], transition_review=evidence["transition_review"],
            ambiguities=[],
        )
        return build_notification_execution_readiness_record(
            request_id=f"{prefix}-{kind}-{seconds}", recorded_at=instant, decision=decision,
        )

    return {
        "plan": plan_for(), "strict_plan": plan_for(max_age=1),
        "initial_plan": plan_for(initial_only=True), "missing_plan": plan_for(missing=True),
        "blocked_plan": plan_for(retry_enabled=False),
        "records": [record_for("initial", 10), record_for("retry", 10)],
        "blocked_record": record_for("retry", 5, retry_enabled=False),
        "corrupt_record": record_for("initial", 3), "oversized_record": record_for("initial", 2),
    }


def _record_row(record: dict[str, Any]) -> dict[str, Any]:
    record = validate_notification_execution_readiness_record(record)
    decision = record["decision"]
    config, destination = decision["configuration"], decision["destination"]
    activation, transition = decision["activation_review"] or {}, decision["transition_review"] or {}
    ambiguity = decision["ambiguity"]
    return {
        "record_id": record["record_id"], "model_version": record["model_version"],
        "request_id": record["request_id"], "decision_id": decision["decision_id"],
        "destination_id": destination["destination_id"], "execution_kind": decision["execution_kind"],
        "evaluated_at": decision["evaluated_at"], "recorded_at": record["recorded_at"],
        "decision": decision["decision"], "blocking_reasons_json": decision["blocking_reasons"],
        "delivery_fingerprint": config["delivery_fingerprint"],
        "retry_policy_fingerprint": config["retry_policy_fingerprint"],
        "retry_execution_policy_fingerprint": config["retry_execution_policy_fingerprint"],
        "endpoint_environment_variable": config["endpoint_environment_variable"],
        "destination_fingerprint": destination["fingerprint"],
        "destination_activation_status": destination["activation_status"],
        "activation_authority_id": activation.get("authority_id"),
        "activation_checklist_id": activation.get("checklist_id"),
        "activation_review_status": activation.get("review_status"),
        "activation_ready": activation.get("operational_activation_ready"),
        "transition_record_id": transition.get("transition_record_id"),
        "transition_rehearsal_id": transition.get("transition_rehearsal_id"),
        "transition_review_status": transition.get("transition_review_status"),
        "transition_ready": transition.get("transition_ready"),
        "ambiguity_count": ambiguity["count"], "ambiguity_event_ids_json": ambiguity["event_ids"],
        "ambiguity_record_ids_json": ambiguity["record_ids"],
        "unbound_ambiguity_event_ids_json": ambiguity["unbound_event_ids"],
        "decision_json": decision, "record_json": record,
        "document_sha256": hashlib.sha256(canonical_bytes(record)).hexdigest(),
        "loaded_at": record["recorded_at"],
    }


def _insert_fixture_record(cursor: Any, record: dict[str, Any], *, corrupt_digest: bool = False,
                           oversized: bool = False) -> None:
    from psycopg.types.json import Jsonb

    row = _record_row(record)
    if oversized:
        row["record_json"] = {**row["record_json"], "fixture_padding": "x" * MAX_SOURCE_BYTES}
        row["document_sha256"] = hashlib.sha256(canonical_bytes(row["record_json"])).hexdigest()
    if corrupt_digest:
        row["document_sha256"] = "0" * 64
    # Direct insertion is deliberate for corruption probes and stays uncommitted.
    # Ordinary application callers must use the reviewed source recorder.
    cursor.execute(
        f"INSERT INTO {TABLE} SELECT (jsonb_populate_record(NULL::{TABLE}, %s::jsonb)).*",
        (Jsonb(row),),
    )


def _expect_read_failure(cursor: Any, plan: dict[str, Any]) -> None:
    try:
        read_worker_readiness_sources_with_cursor(cursor, plan=plan)
    except StorageError as exc:
        if str(exc) != "unable to capture verified worker readiness sources":
            raise AssertionError("source failure was not redacted") from exc
    else:
        raise AssertionError("invalid retained source was accepted")


def prove_worker_readiness_sources(*, dsn: str, destination: NotificationDestination,
                                  evidence: dict[str, Any], now: datetime) -> tuple[dict[str, Any], dict[str, bool]]:
    """Extend the existing disposable readiness fixture; always roll back new rows."""
    import psycopg

    prefix = "worker-source-proof-" + uuid4().hex
    fixture = _fixture_inputs(destination=destination, evidence=evidence, now=now, prefix=prefix)
    plan = fixture["plan"]
    baseline = read_worker_readiness_sources(dsn=dsn, plan=plan)
    baseline_ids = [row["readiness_record_id"] for row in baseline["sources"]]
    proofs: dict[str, bool] = {}
    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                for record in fixture["records"]:
                    _insert_fixture_record(cursor, record)
                captured = read_worker_readiness_sources_with_cursor(cursor, plan=plan)
                if (captured["all_sources_allowed"] is not True
                        or [row["record_id"] for row in captured["readiness"]]
                        != [record["record_id"] for record in fixture["records"]]
                        or validate_worker_readiness_sources(captured) != captured):
                    raise AssertionError("real source capture failed canonical reconciliation")
                proofs["both_kinds_and_canonical_records"] = True
                initial = read_worker_readiness_sources_with_cursor(cursor, plan=fixture["initial_plan"])
                if len(initial["readiness"]) != 1 or initial["all_sources_allowed"] is not True:
                    raise AssertionError("initial-only query did not isolate its kind")
                proofs["initial_only_grain"] = True
                strict = read_worker_readiness_sources_with_cursor(cursor, plan=fixture["strict_plan"])
                if any(row["status"] != "stale" for row in strict["readiness"]):
                    raise AssertionError("stricter worker freshness was not enforced")
                proofs["worker_specific_freshness"] = True
                missing = read_worker_readiness_sources_with_cursor(cursor, plan=fixture["missing_plan"])
                if any(row["status"] != "missing" for row in missing["readiness"]) or not missing["observed_at"]:
                    raise AssertionError("missing sources lost their clock or became healthy")
                proofs["clocked_missing_sources"] = True
                isolated = read_worker_readiness_sources(dsn=dsn, plan=plan)
                if [row["readiness_record_id"] for row in isolated["sources"]] != baseline_ids:
                    raise AssertionError("public reader observed uncommitted source fixtures")
                proofs["public_reader_uncommitted_isolation"] = True
                _insert_fixture_record(cursor, fixture["blocked_record"])
                blocked = read_worker_readiness_sources_with_cursor(cursor, plan=fixture["blocked_plan"])
                if blocked["readiness"][1]["status"] != "blocked" or blocked["all_sources_allowed"] is not False:
                    raise AssertionError("retained blocked retry became allowed")
                proofs["retained_blocked_source"] = True
                for oversize in (False, True):
                    cursor.execute("SAVEPOINT worker_source_corruption")
                    try:
                        _insert_fixture_record(
                            cursor, fixture["oversized_record"] if oversize else fixture["corrupt_record"],
                            corrupt_digest=not oversize, oversized=oversize,
                        )
                        if oversize:
                            cursor.execute(READINESS_SOURCES_SQL, (["initial", "retry"], MAX_SOURCE_BYTES,
                                                                 plan["destination"]["destination_id"]))
                            row = cursor.fetchone()
                            if row is None or row[7] is not None or row[9] <= MAX_SOURCE_BYTES:
                                raise AssertionError("oversized JSON was not withheld by the SELECT")
                        _expect_read_failure(cursor, plan)
                    finally:
                        cursor.execute("ROLLBACK TO SAVEPOINT worker_source_corruption")
                        cursor.execute("RELEASE SAVEPOINT worker_source_corruption")
                proofs["corrupt_digest_rejected"] = True
                proofs["oversize_withheld_and_rejected"] = True
        finally:
            connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE request_id LIKE %s", (prefix + "%",))
            if cursor.fetchone() != (0,):
                raise AssertionError("readiness source fixture records survived rollback")
    after = read_worker_readiness_sources(dsn=dsn, plan=plan)
    if [row["readiness_record_id"] for row in after["sources"]] != baseline_ids:
        raise AssertionError("source proof changed the original fixture heads")
    proofs["fixture_rollback_preserves_original_heads"] = True
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(f"UPDATE {TABLE} SET loaded_at = loaded_at WHERE FALSE")
    except psycopg.errors.ReadOnlySqlTransaction:
        proofs["postgres_read_only_rejects_writes"] = True
    else:
        raise AssertionError("PostgreSQL READ ONLY accepted an ordinary table write")
    return plan, proofs


def prove_worker_readiness_supersession(*, dsn: str, plan: dict[str, Any]) -> bool:
    captured = read_worker_readiness_sources(dsn=dsn, plan=plan)
    if not all(row["status"] == "superseded" and "serving_evidence_superseded" in row["reasons"]
               for row in captured["readiness"]):
        raise AssertionError("new destination evidence did not supersede both readiness sources")
    return True
