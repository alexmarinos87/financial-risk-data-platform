from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.local_schedule_run_contract_check import (
    run_contract_check as run_local_schedule_run_contract_check,
)
from src.warehouse.operational_readiness_override_registry import (
    approve_operational_readiness_override,
    read_active_operational_readiness_override,
    revoke_operational_readiness_override,
)
from src.warehouse.postgres_consistency import run_consistency_checks


def run_contract_check(
    *,
    dsn: str,
    blocked_decision_id: str,
    allowed_decision_id: str,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    first_approved_at = now - timedelta(minutes=5)
    first_expires_at = now + timedelta(minutes=30)
    first = approve_operational_readiness_override(
        dsn=dsn,
        decision_id=blocked_decision_id,
        request_identifier="READINESS-OVERRIDE-001",
        approved_at=first_approved_at,
        expires_at=first_expires_at,
        approved_by="operator@example.test",
        reason="Reviewed local execution exception.",
    )
    replay = approve_operational_readiness_override(
        dsn=dsn,
        decision_id=blocked_decision_id,
        request_identifier="READINESS-OVERRIDE-001",
        approved_at=first_approved_at,
        expires_at=first_expires_at,
        approved_by="operator@example.test",
        reason="Reviewed local execution exception.",
    )
    if first["created"] is not True or replay["created"] is not False:
        raise AssertionError("readiness override retry did not converge")

    try:
        approve_operational_readiness_override(
            dsn=dsn,
            decision_id=blocked_decision_id,
            request_identifier="READINESS-OVERRIDE-001",
            approved_at=first_approved_at,
            expires_at=first_expires_at + timedelta(minutes=1),
            approved_by="operator@example.test",
            reason="Reviewed local execution exception.",
        )
    except ValidationError:
        pass
    else:
        raise AssertionError("conflicting override request was accepted")

    try:
        approve_operational_readiness_override(
            dsn=dsn,
            decision_id=allowed_decision_id,
            request_identifier="READINESS-OVERRIDE-ALLOW",
            approved_at=first_approved_at,
            expires_at=first_expires_at,
            approved_by="operator@example.test",
            reason="An allow decision must not accept override evidence.",
        )
    except ValidationError:
        pass
    else:
        raise AssertionError("allow decision accepted override evidence")

    revoked_at = now - timedelta(minutes=3)
    revocation = revoke_operational_readiness_override(
        dsn=dsn,
        override_id=str(first["override_id"]),
        request_identifier="READINESS-OVERRIDE-REVOKE-001",
        revoked_at=revoked_at,
        revoked_by="operator@example.test",
        reason="First override withdrawn after review.",
    )
    revocation_replay = revoke_operational_readiness_override(
        dsn=dsn,
        override_id=str(first["override_id"]),
        request_identifier="READINESS-OVERRIDE-REVOKE-001",
        revoked_at=revoked_at,
        revoked_by="operator@example.test",
        reason="First override withdrawn after review.",
    )
    if (
        revocation["created"] is not True
        or revocation_replay["created"] is not False
    ):
        raise AssertionError("readiness override revocation retry did not converge")

    second = approve_operational_readiness_override(
        dsn=dsn,
        decision_id=blocked_decision_id,
        request_identifier="READINESS-OVERRIDE-002",
        approved_at=now - timedelta(minutes=2),
        expires_at=now + timedelta(minutes=45),
        approved_by="operator@example.test",
        reason="Replacement override approved for one bounded local window.",
    )
    active = read_active_operational_readiness_override(
        dsn=dsn,
        decision_id=blocked_decision_id,
        evaluated_at=now,
    )
    if active is None or active["override_id"] != second["override_id"]:
        raise AssertionError("current active readiness override was not selected")
    if (
        read_active_operational_readiness_override(
            dsn=dsn,
            decision_id=blocked_decision_id,
            evaluated_at=now + timedelta(hours=1),
        )
        is not None
    ):
        raise AssertionError("expired readiness override remained active")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Readiness override contract requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_readiness_overrides),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_readiness_override_revocations),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_readiness_override_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_readiness_override_status),
                    (SELECT COUNT(*)
                     FROM risk_platform.active_operational_readiness_overrides),
                    (SELECT override_id
                     FROM risk_platform.active_operational_readiness_overrides)
                """
            )
            counts = cursor.fetchone()
            if counts != (2, 1, 3, 1, 1, second["override_id"]):
                raise AssertionError(
                    f"readiness override serving views are incompatible: {counts!r}"
                )

    mutation_statements = (
        (
            """
            UPDATE risk_platform.operational_readiness_overrides
            SET created_at = created_at
            WHERE override_id = %s
            """,
            first["override_id"],
        ),
        (
            """
            DELETE FROM risk_platform.operational_readiness_overrides
            WHERE override_id = %s
            """,
            first["override_id"],
        ),
        (
            """
            UPDATE risk_platform.operational_readiness_override_revocations
            SET created_at = created_at
            WHERE revocation_id = %s
            """,
            revocation["revocation_id"],
        ),
        (
            """
            DELETE FROM risk_platform.operational_readiness_override_revocations
            WHERE revocation_id = %s
            """,
            revocation["revocation_id"],
        ),
    )
    for statement, identifier in mutation_statements:
        with psycopg.connect(dsn) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(statement, (identifier,))
            except psycopg.Error:
                connection.rollback()
            else:
                raise AssertionError("readiness override mutation was not blocked")

    consistency = run_consistency_checks(
        dsn=dsn,
        check_paths=(
            Path("sql/operational_readiness_overrides_consistency_checks.sql"),
        ),
    )
    failures = [result for result in consistency if result.status != "pass"]
    if failures:
        names = ", ".join(result.check_name for result in failures)
        raise AssertionError("readiness override reconciliation failed: " + names)

    local_run_result = run_local_schedule_run_contract_check(
        dsn=dsn,
        allowed_decision_id=allowed_decision_id,
        blocked_decision_id=blocked_decision_id,
        active_override_id=str(second["override_id"]),
    )

    return {
        "override_rows": 2,
        "revocation_rows": 1,
        "active_override_id": second["override_id"],
        "retry_verified": True,
        "conflict_verified": True,
        "allow_target_rejected": True,
        "expiry_verified": True,
        "append_only_verified": True,
        "consistency_checks": len(consistency),
        "local_schedule_run_contract": local_run_result,
    }
