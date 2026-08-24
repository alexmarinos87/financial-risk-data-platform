-- Reconciliation checks for append-only operational readiness override evidence.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.operational_readiness_overrides) AS override_rows,
        (SELECT COUNT(*)
         FROM risk_platform.operational_readiness_override_revocations)
            AS revocation_rows,
        (SELECT COUNT(*)
         FROM risk_platform.operational_readiness_override_history)
            AS history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_readiness_override_status)
            AS current_rows,
        (SELECT COUNT(*)
         FROM risk_platform.active_operational_readiness_overrides)
            AS active_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_readiness_override_status
         WHERE override_status = 'active') AS expected_active_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_overrides override
            LEFT JOIN risk_platform.operational_readiness_decisions decision
                ON decision.decision_id = override.decision_id
            WHERE decision.decision_id IS NULL
        ) AS orphan_decisions,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_overrides override
            JOIN risk_platform.operational_readiness_decisions decision
                ON decision.decision_id = override.decision_id
            WHERE decision.decision <> 'block'
               OR override.decision_document_sha256 <> decision.document_sha256
               OR override.gate_id <> decision.gate_id
               OR override.gate_fingerprint <> decision.gate_fingerprint
               OR override.operational_policy_id
                    <> decision.operational_policy_id
               OR override.operational_policy_fingerprint
                    <> decision.operational_policy_fingerprint
               OR override.schedule_id <> decision.schedule_id
               OR override.schedule_fingerprint <> decision.schedule_fingerprint
               OR override.calendar_id <> decision.calendar_id
               OR override.portfolio_id <> decision.portfolio_id
               OR override.risk_limit_policy_id
                    <> decision.risk_limit_policy_id
               OR override.mandate_fingerprint <> decision.mandate_fingerprint
               OR override.latest_expected_session
                    <> decision.latest_expected_session
               OR override.approved_at < decision.evaluated_at
        ) AS invalid_target_contracts,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_overrides override
            WHERE override.expires_at <= override.approved_at
               OR override.expires_at - override.approved_at
                    > INTERVAL '24 hours'
        ) AS invalid_override_windows,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_override_revocations revocation
            LEFT JOIN risk_platform.operational_readiness_overrides override
                ON override.override_id = revocation.override_id
            WHERE override.override_id IS NULL
               OR revocation.revoked_at < override.approved_at
        ) AS invalid_revocations,
        (
            SELECT COUNT(*)
            FROM (
                SELECT override_id
                FROM risk_platform.operational_readiness_overrides
                GROUP BY override_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_override_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT request_id
                FROM risk_platform.operational_readiness_overrides
                GROUP BY request_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_override_requests,
        (
            SELECT COUNT(*)
            FROM (
                SELECT revocation_id
                FROM risk_platform.operational_readiness_override_revocations
                GROUP BY revocation_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_revocation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT request_id
                FROM risk_platform.operational_readiness_override_revocations
                GROUP BY request_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_revocation_requests,
        (
            SELECT COUNT(*)
            FROM (
                SELECT decision_id
                FROM risk_platform.current_operational_readiness_override_status
                GROUP BY decision_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_current_decisions,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_operational_readiness_override_status current
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.operational_readiness_overrides candidate
                WHERE candidate.decision_id = current.decision_id
                  AND (
                        candidate.approved_at,
                        candidate.override_id
                  ) > (
                        current.approved_at,
                        current.override_id
                  )
            )
        ) AS stale_current_overrides,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_operational_readiness_override_status current
            WHERE current.override_status <> CASE
                WHEN current.approved_at > CURRENT_TIMESTAMP THEN 'pending'
                WHEN current.revoked_at IS NOT NULL
                     AND current.revoked_at <= CURRENT_TIMESTAMP THEN 'revoked'
                WHEN current.expires_at <= CURRENT_TIMESTAMP THEN 'expired'
                ELSE 'active'
            END
        ) AS invalid_current_statuses,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger
            JOIN pg_class table_ref ON table_ref.oid = trigger.tgrelid
            JOIN pg_namespace namespace_ref
                ON namespace_ref.oid = table_ref.relnamespace
            WHERE namespace_ref.nspname = 'risk_platform'
              AND trigger.tgname IN (
                    'prevent_operational_readiness_override_update',
                    'prevent_operational_readiness_override_delete',
                    'prevent_operational_readiness_override_revocation_update',
                    'prevent_operational_readiness_override_revocation_delete'
              )
              AND NOT trigger.tgisinternal
        ) AS append_only_trigger_count
)
SELECT
    'operational_readiness_override_history_rows_match' AS check_name,
    (override_rows + revocation_rows)::TEXT AS expected,
    history_rows::TEXT AS actual,
    CASE
        WHEN override_rows + revocation_rows = history_rows THEN 'pass'
        ELSE 'fail'
    END AS status
FROM counts

UNION ALL

SELECT
    'operational_readiness_override_targets_exist',
    '0',
    orphan_decisions::TEXT,
    CASE WHEN orphan_decisions = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_override_targets_blocked_exact_contract',
    '0',
    invalid_target_contracts::TEXT,
    CASE WHEN invalid_target_contracts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_override_windows_bounded',
    '0',
    invalid_override_windows::TEXT,
    CASE WHEN invalid_override_windows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_override_revocations_valid',
    '0',
    invalid_revocations::TEXT,
    CASE WHEN invalid_revocations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_override_identities_unique',
    '0',
    (duplicate_override_ids + duplicate_override_requests)::TEXT,
    CASE
        WHEN duplicate_override_ids + duplicate_override_requests = 0
            THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_override_revocation_identities_unique',
    '0',
    (duplicate_revocation_ids + duplicate_revocation_requests)::TEXT,
    CASE
        WHEN duplicate_revocation_ids + duplicate_revocation_requests = 0
            THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'current_operational_readiness_override_grain_unique',
    '0',
    duplicate_current_decisions::TEXT,
    CASE WHEN duplicate_current_decisions = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_operational_readiness_override_selects_latest',
    '0',
    stale_current_overrides::TEXT,
    CASE WHEN stale_current_overrides = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_operational_readiness_override_status_reconciles',
    '0',
    invalid_current_statuses::TEXT,
    CASE WHEN invalid_current_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'active_operational_readiness_overrides_match_current_status',
    expected_active_rows::TEXT,
    active_rows::TEXT,
    CASE WHEN expected_active_rows = active_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'operational_readiness_override_append_only_triggers_present',
    '4',
    append_only_trigger_count::TEXT,
    CASE WHEN append_only_trigger_count = 4 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
