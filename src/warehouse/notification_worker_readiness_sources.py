"""Verify retained readiness sources; never infer full worker health or permission."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes, utc
from src.orchestration.notification_worker_plan_validation import exact_object
from src.orchestration.plan_notification_worker import validate_notification_worker_plan
from src.warehouse.notification_execution_readiness_history_contract import (
    validate_notification_execution_readiness_record,
)

MODEL_VERSION = "portfolio-risk-worker-readiness-sources-v1"
MAX_SOURCE_BYTES = 262_144
MAX_SNAPSHOT_BYTES = 1_048_576
SOURCE_FIELDS = frozenset({
    "destination_id", "execution_kind", "readiness_record_id", "readiness_review_status",
    "execution_ready", "decision_matches_current_evidence", "record_json", "document_sha256",
})
SNAPSHOT_FIELDS = frozenset({
    "snapshot_id", "model_version", "plan", "observed_at", "sources", "readiness",
    "all_sources_allowed", "worker_authority_verified", "failure_history_verified",
    "runtime_permission_granted",
})
PRESENT_STATUSES = {"allowed", "blocked", "decision_stale", "decision_superseded"}


def _projection(kind: str, source: dict[str, Any] | None, plan: dict[str, Any],
                observed: datetime) -> dict[str, Any]:
    missing = {
        "execution_kind": kind, "record_id": None, "document_sha256": None,
        "evaluated_at": None, "status": "missing", "configuration_matches": False,
        "reasons": ["readiness_missing"],
    }
    if source is None:
        return missing
    status = source["readiness_review_status"]
    if not isinstance(status, str) or status not in PRESENT_STATUSES | {"decision_missing"}:
        raise ValidationError("readiness source status is invalid")
    for flag in ("execution_ready", "decision_matches_current_evidence"):
        if type(source[flag]) is not bool:
            raise ValidationError("readiness source flags must be boolean")
    if source["destination_id"] != plan["destination"]["destination_id"]:
        raise ValidationError("readiness source destination differs from the plan")
    if source["readiness_record_id"] is None:
        if (status != "decision_missing" or source["record_json"] is not None
                or source["document_sha256"] is not None or source["execution_ready"]
                or source["decision_matches_current_evidence"]):
            raise ValidationError("missing readiness source has contradictory evidence")
        return missing
    if (status not in PRESENT_STATUSES
            or source["execution_ready"] is not (status == "allowed")
            or source["decision_matches_current_evidence"] is not (status != "decision_superseded")):
        raise ValidationError("readiness source status contradicts serving flags")
    encoded = canonical_bytes(source["record_json"])
    if len(encoded) > MAX_SOURCE_BYTES:
        raise ValidationError("readiness source record exceeds the byte limit")
    record = validate_notification_execution_readiness_record(source["record_json"])
    if canonical_bytes(record) != encoded:
        raise ValidationError("readiness source record is not canonical")
    digest = hashlib.sha256(encoded).hexdigest()
    if digest != source["document_sha256"] or record["record_id"] != source["readiness_record_id"]:
        raise ValidationError("readiness source identity or digest differs")
    decision = record["decision"]
    if (decision["execution_kind"] != kind
            or decision["destination"]["destination_id"] != source["destination_id"]):
        raise ValidationError("readiness source record scope differs from its serving row")
    evaluated = utc(decision["evaluated_at"], "readiness evaluated_at")
    recorded = utc(record["recorded_at"], "readiness recorded_at")
    if not evaluated <= recorded <= observed:
        raise ValidationError("readiness source chronology is invalid")
    if ((status == "allowed" and decision["decision"] != "allow")
            or (status == "blocked" and decision["decision"] != "block")):
        raise ValidationError("readiness source status contradicts its retained decision")
    configuration = decision["configuration"]
    delivery = plan["delivery"]
    destination = plan["destination"]
    matches = all(configuration[key] == expected for key, expected in (
        ("delivery_fingerprint", delivery["delivery_fingerprint"]),
        ("retry_policy_fingerprint", delivery["retry_planning_policy_fingerprint"]),
        ("retry_execution_policy_fingerprint", delivery["retry_execution_policy_fingerprint"]),
        ("endpoint_environment_variable", destination["endpoint_environment_variable"]),
        ("delivery_enabled", delivery["webhook_enabled"]),
        ("retry_execution_enabled", delivery["retry_execution_enabled"]),
    )) and all(decision["destination"][key] == expected for key, expected in (
        ("fingerprint", destination["fingerprint"]),
        ("endpoint_environment_variable", destination["endpoint_environment_variable"]),
    ))
    reasons = []
    if status == "decision_superseded":
        reasons.append("serving_evidence_superseded")
    if not matches:
        reasons.append("configuration_mismatch")
    stale = (observed - evaluated).total_seconds() > plan["readiness"]["max_age_seconds"]
    if stale or status == "decision_stale":
        reasons.append("readiness_stale")
    if decision["decision"] == "block":
        reasons.append("retained_decision_blocked")
    normalized = ("superseded" if not matches or status == "decision_superseded" else
                  "stale" if stale or status == "decision_stale" else
                  "blocked" if decision["decision"] == "block" else "allowed")
    return {
        "execution_kind": kind, "record_id": record["record_id"],
        "document_sha256": digest, "evaluated_at": evaluated.isoformat(),
        "status": normalized, "configuration_matches": matches, "reasons": reasons,
    }


def build_worker_readiness_sources(
    *, plan: Mapping[str, Any], sources: Sequence[Mapping[str, Any]],
    observed_at: datetime | str,
) -> dict[str, Any]:
    """Reconcile supplied serving rows and full records, not their authenticity.

    Source selection must be performed by a trusted read adapter. A passing
    source set neither establishes current worker authority nor proves complete
    failure history. This function performs no I/O and grants no permission.
    """
    try:
        selected = validate_notification_worker_plan(plan)
        observed = utc(observed_at, "observed_at")
        kinds = [item["execution_kind"] for item in selected["execution"]["work_items"]]
        if not isinstance(sources, (list, tuple)) or len(sources) > len(kinds):
            raise ValidationError("readiness sources must be a bounded sequence")
        rows = [exact_object(row, SOURCE_FIELDS, "readiness source") for row in sources]
        raw = canonical_bytes({"sources": rows})
        if len(raw) > MAX_SNAPSHOT_BYTES:
            raise ValidationError("readiness sources exceed the byte limit")
        rows = json.loads(raw)["sources"]
        names = [row["execution_kind"] for row in rows]
        if (any(not isinstance(name, str) or name not in kinds for name in names)
                or names != sorted(set(names))):
            raise ValidationError("readiness source kinds must be selected, sorted and unique")
        indexed = dict(zip(names, rows, strict=True))
        readiness = [_projection(kind, indexed.get(kind), selected, observed) for kind in kinds]
        identity = {
            "model_version": MODEL_VERSION, "plan": selected, "observed_at": observed.isoformat(),
            "sources": rows, "readiness": readiness,
            "all_sources_allowed": all(row["status"] == "allowed" for row in readiness),
            "worker_authority_verified": False, "failure_history_verified": False,
            "runtime_permission_granted": False,
        }
        digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
        result = {"snapshot_id": f"{MODEL_VERSION}-{digest}", **identity}
        if len(canonical_bytes(result)) > MAX_SNAPSHOT_BYTES:
            raise ValidationError("readiness source snapshot exceeds the byte limit")
        return result
    except (TypeError, ValueError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker readiness source evidence is malformed") from None


def validate_worker_readiness_sources(value: Mapping[str, Any]) -> dict[str, Any]:
    try:
        source = exact_object(value, SNAPSHOT_FIELDS, "readiness source snapshot")
        encoded = canonical_bytes(source)
        if len(encoded) > MAX_SNAPSHOT_BYTES:
            raise ValidationError("readiness source snapshot exceeds the byte limit")
        source = json.loads(encoded)
        rebuilt = build_worker_readiness_sources(
            plan=source["plan"], sources=source["sources"], observed_at=source["observed_at"],
        )
        if encoded != canonical_bytes(rebuilt):
            raise ValidationError("readiness source snapshot differs from canonical evidence")
        return rebuilt
    except (TypeError, ValueError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker readiness source snapshot is malformed") from None
