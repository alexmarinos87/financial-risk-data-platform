-- Reconciliation checks for append-only operational readiness decisions.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.operational_readiness_decisions) AS decision_rows,
        (SELECT COUNT(*)
         FROM risk_platform.operational_readiness_decision_history) AS history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.operational_readiness_reason_history) AS reason_rows,
        (SELECT COALESCE(SUM(jsonb_array_length(reasons)), 0)
         FROM risk_platform.operational_readiness_decisions) AS expected_reason_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_operational_readiness_decisions) AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_allowed_operational_readiness_decisions)
            AS allowed_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_blocked_operational_readiness_decisions)
            AS blocked_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_decisions decision
            LEFT JOIN risk_platform.operational_service_level_reports report
                ON report.calculation_id = decision.report_calculation_id
            WHERE decision.report_calculation_id IS NOT NULL
              AND report.calculation_id IS NULL
        ) AS orphan_report_references,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_decisions decision
            JOIN risk_platform.operational_service_level_reports report
                ON report.calculation_id = decision.report_calculation_id
            WHERE decision.report_document_sha256 <> report.document_sha256
               OR decision.operational_policy_id <> report.policy_id
               OR decision.operational_policy_fingerprint <> report.policy_fingerprint
               OR decision.schedule_id <> report.schedule_id
               OR decision.schedule_fingerprint <> report.schedule_fingerprint
               OR decision.calendar_id <> report.calendar_id
               OR decision.portfolio_id <> report.portfolio_id
               OR decision.risk_limit_policy_id <> report.risk_limit_policy_id
               OR decision.mandate_fingerprint <> report.mandate_fingerprint
               OR decision.report_as_of <> report.as_of
               OR decision.report_latest_expected_session
                    <> report.latest_expected_session
               OR decision.report_status <> report.overall_status
        ) AS mismatched_report_evidence,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_decisions decision
            WHERE decision.report_calculation_id IS NOT NULL
              AND (
                    ABS(
                        decision.report_age_seconds
                        - GREATEST(
                            0,
                            EXTRACT(EPOCH FROM (
                                decision.evaluated_at - decision.report_as_of
                            ))
                        )
                    ) > 0.000001
                    OR ABS(
                        decision.report_future_seconds
                        - GREATEST(
                            0,
                            EXTRACT(EPOCH FROM (
                                decision.report_as_of - decision.evaluated_at
                            ))
                        )
                    ) > 0.000001
              )
        ) AS invalid_report_age_evidence,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_decisions decision
            WHERE
                (decision.report_calculation_id IS NULL)
                    <> (decision.reasons = '["report_missing"]'::JSONB)
                OR (
                    decision.report_calculation_id IS NOT NULL
                    AND (
                        (decision.report_future_seconds > 0)
                            <> (decision.reasons ? 'report_timestamp_future')
                        OR (
                            decision.report_future_seconds = 0
                            AND decision.report_age_seconds
                                > decision.max_report_age_seconds
                        ) <> (decision.reasons ? 'report_age_exceeds_limit')
                        OR (
                            decision.report_latest_expected_session
                                <> decision.latest_expected_session
                        ) <> (decision.reasons ? 'report_session_mismatch')
                        OR (decision.report_status = 'critical')
                            <> (decision.reasons ? 'report_status_critical')
                        OR (
                            decision.report_status = 'warning'
                            AND NOT decision.allow_warning
                        ) <> (decision.reasons ? 'report_status_warning')
                    )
                )
                OR (decision.decision = 'allow')
                    <> (jsonb_array_length(decision.reasons) = 0)
        ) AS invalid_reason_semantics,
        (
            SELECT COUNT(*)
            FROM (
                SELECT decision_id
                FROM risk_platform.operational_readiness_decisions
                GROUP BY decision_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_decision_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    model_version,
                    gate_id,
                    gate_fingerprint,
                    operational_policy_id,
                    operational_policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    latest_expected_session
                FROM risk_platform.latest_operational_readiness_decisions
                GROUP BY
                    model_version,
                    gate_id,
                    gate_fingerprint,
                    operational_policy_id,
                    operational_policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    latest_expected_session
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_operational_readiness_decisions latest
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.operational_readiness_decisions candidate
                WHERE candidate.model_version = latest.model_version
                  AND candidate.gate_id = latest.gate_id
                  AND candidate.gate_fingerprint = latest.gate_fingerprint
                  AND candidate.operational_policy_id
                        = latest.operational_policy_id
                  AND candidate.operational_policy_fingerprint
                        = latest.operational_policy_fingerprint
                  AND candidate.schedule_id = latest.schedule_id
                  AND candidate.schedule_fingerprint
                        = latest.schedule_fingerprint
                  AND candidate.calendar_id = latest.calendar_id
                  AND candidate.portfolio_id = latest.portfolio_id
                  AND candidate.risk_limit_policy_id
                        = latest.risk_limit_policy_id
                  AND candidate.mandate_fingerprint
                        = latest.mandate_fingerprint
                  AND candidate.latest_expected_session
                        = latest.latest_expected_session
                  AND (
                        candidate.evaluated_at,
                        candidate.decision_id
                  ) > (
                        latest.evaluated_at,
                        latest.decision_id
                  )
            )
        ) AS stale_latest_rows,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger
            JOIN pg_class table_ref ON table_ref.oid = trigger.tgrelid
            JOIN pg_namespace namespace_ref
                ON namespace_ref.oid = table_ref.relnamespace
            WHERE namespace_ref.nspname = 'risk_platform'
              AND table_ref.relname = 'operational_readiness_decisions'
              AND trigger.tgname IN (
                    'prevent_operational_readiness_decision_update',
                    'prevent_operational_readiness_decision_delete'
              )
              AND NOT trigger.tgisinternal
        ) AS append_only_trigger_count
)
SELECT
    'operational_readiness_history_rows_match' AS check_name,
    decision_rows::TEXT AS expected,
    history_rows::TEXT AS actual,
    CASE WHEN decision_rows = history_rows THEN 'pass' ELSE 'fail' END AS status
FROM counts

UNION ALL

SELECT
    'operational_readiness_reason_rows_match',
    expected_reason_rows::TEXT,
    reason_rows::TEXT,
    CASE WHEN expected_reason_rows = reason_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'operational_readiness_current_views_partition_latest',
    latest_rows::TEXT,
    (allowed_rows + blocked_rows)::TEXT,
    CASE WHEN latest_rows = allowed_rows + blocked_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'operational_readiness_report_references_exist',
    '0',
    orphan_report_references::TEXT,
    CASE WHEN orphan_report_references = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_report_evidence_matches_source',
    '0',
    mismatched_report_evidence::TEXT,
    CASE WHEN mismatched_report_evidence = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_report_age_reconciles',
    '0',
    invalid_report_age_evidence::TEXT,
    CASE WHEN invalid_report_age_evidence = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_reason_semantics_reconcile',
    '0',
    invalid_reason_semantics::TEXT,
    CASE WHEN invalid_reason_semantics = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_decision_ids_unique',
    '0',
    duplicate_decision_ids::TEXT,
    CASE WHEN duplicate_decision_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_operational_readiness_grain_unique',
    '0',
    duplicate_latest_grains::TEXT,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_operational_readiness_selects_current_decision',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_readiness_append_only_triggers_present',
    '2',
    append_only_trigger_count::TEXT,
    CASE WHEN append_only_trigger_count = 2 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
