-- Reconciliation checks for append-only readiness-authorised local schedule runs.

WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM risk_platform.local_schedule_runs) AS run_rows,
        (SELECT COALESCE(SUM(selected_session_count), 0)
         FROM risk_platform.local_schedule_runs) AS expected_session_rows,
        (SELECT COUNT(*)
         FROM risk_platform.local_schedule_run_session_history) AS session_rows,
        (SELECT COALESCE(SUM(jsonb_array_length(session.value -> 'stages')), 0)
         FROM risk_platform.local_schedule_runs run
         CROSS JOIN LATERAL jsonb_array_elements(run.run_json -> 'sessions')
            AS session(value)) AS expected_stage_rows,
        (SELECT COUNT(*)
         FROM risk_platform.local_schedule_run_stage_history) AS stage_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_local_schedule_run_status) AS current_rows,
        (SELECT COUNT(DISTINCT schedule_id)
         FROM risk_platform.local_schedule_runs) AS expected_current_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_local_schedule_run_failures)
            AS current_failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_local_schedule_run_status
         WHERE run_status = 'failed') AS expected_current_failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.incomplete_local_schedule_sessions)
            AS incomplete_session_rows,
        (SELECT COUNT(*)
         FROM risk_platform.local_schedule_run_session_history
         WHERE session_status IN ('selected', 'failed'))
            AS expected_incomplete_session_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.local_schedule_runs run
            LEFT JOIN risk_platform.operational_readiness_decisions decision
                ON decision.decision_id = run.readiness_decision_id
            WHERE decision.decision_id IS NULL
        ) AS orphan_decisions,
        (
            SELECT COUNT(*)
            FROM risk_platform.local_schedule_runs run
            JOIN risk_platform.operational_readiness_decisions decision
                ON decision.decision_id = run.readiness_decision_id
            WHERE run.readiness_document_sha256 <> decision.document_sha256
               OR run.schedule_id <> decision.schedule_id
               OR run.schedule_fingerprint <> decision.schedule_fingerprint
               OR run.calendar_id <> decision.calendar_id
               OR run.portfolio_id <> decision.portfolio_id
               OR run.risk_limit_policy_id <> decision.risk_limit_policy_id
               OR run.mandate_fingerprint <> decision.mandate_fingerprint
               OR run.latest_expected_session <> decision.latest_expected_session
               OR (run.authority_type = 'gate_allow'
                   AND (decision.decision <> 'allow' OR run.override_id IS NOT NULL))
               OR (run.authority_type = 'active_override'
                   AND (decision.decision <> 'block' OR run.override_id IS NULL))
        ) AS invalid_decision_contracts,
        (
            SELECT COUNT(*)
            FROM risk_platform.local_schedule_runs run
            LEFT JOIN risk_platform.operational_readiness_overrides override
                ON override.override_id = run.override_id
            WHERE run.authority_type = 'active_override'
              AND (
                    override.override_id IS NULL
                    OR override.decision_id <> run.readiness_decision_id
                    OR override.decision_document_sha256
                        <> run.readiness_document_sha256
                    OR override.schedule_id <> run.schedule_id
                    OR override.schedule_fingerprint <> run.schedule_fingerprint
                    OR override.calendar_id <> run.calendar_id
                    OR override.portfolio_id <> run.portfolio_id
                    OR override.risk_limit_policy_id <> run.risk_limit_policy_id
                    OR override.mandate_fingerprint <> run.mandate_fingerprint
                    OR override.latest_expected_session
                        <> run.latest_expected_session
                    OR NOT (
                        override.approved_at <= run.started_at
                        AND run.started_at < override.expires_at
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM risk_platform.operational_readiness_override_revocations
                            revocation
                        WHERE revocation.override_id = run.override_id
                          AND revocation.revoked_at <= run.started_at
                    )
              )
        ) AS invalid_override_contracts,
        (
            SELECT COUNT(*)
            FROM risk_platform.local_schedule_runs run
            WHERE run.selected_session_count
                    <> jsonb_array_length(run.run_json -> 'sessions')
               OR run.started_session_count <> (
                    SELECT COUNT(*)
                    FROM jsonb_array_elements(run.run_json -> 'sessions') session
                    WHERE session ->> 'status' IN ('completed', 'failed')
               )
               OR run.completed_session_count <> (
                    SELECT COUNT(*)
                    FROM jsonb_array_elements(run.run_json -> 'sessions') session
                    WHERE session ->> 'status' = 'completed'
               )
        ) AS invalid_run_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.local_schedule_run_session_history session
            WHERE (session.session_status = 'completed'
                   AND session.checkpoint_after <> session.session_date)
               OR (session.session_status <> 'completed'
                   AND session.checkpoint_after IS NOT NULL)
               OR (session.session_status = 'selected'
                   AND (session.started_at IS NOT NULL
                        OR session.finished_at IS NOT NULL
                        OR session.attempted_stage_count <> 0))
               OR (session.session_status IN ('completed', 'failed')
                   AND (session.started_at IS NULL
                        OR session.finished_at IS NULL
                        OR session.attempted_stage_count = 0))
        ) AS invalid_session_outcomes,
        (
            SELECT COUNT(*)
            FROM risk_platform.local_schedule_run_stage_history stage
            WHERE stage.stage_index < 0
               OR stage.stage_status NOT IN ('completed', 'failed')
               OR stage.finished_at < stage.started_at
               OR (stage.stage_status = 'completed' AND stage.failure_code IS NOT NULL)
               OR (stage.stage_status = 'failed' AND stage.failure_code IS NULL)
        ) AS invalid_stage_outcomes,
        (
            SELECT COUNT(*)
            FROM (
                SELECT run_id FROM risk_platform.local_schedule_runs
                GROUP BY run_id HAVING COUNT(*) > 1
            ) duplicate_ids
        ) AS duplicate_run_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT request_id FROM risk_platform.local_schedule_runs
                GROUP BY request_id HAVING COUNT(*) > 1
            ) duplicate_requests
        ) AS duplicate_requests,
        (
            SELECT COUNT(*)
            FROM (
                SELECT schedule_id
                FROM risk_platform.current_local_schedule_run_status
                GROUP BY schedule_id
                HAVING COUNT(*) > 1
            ) duplicate_current_schedules
        ) AS duplicate_current_schedules,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_local_schedule_run_status current
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.local_schedule_runs candidate
                WHERE candidate.schedule_id = current.schedule_id
                  AND (candidate.started_at, candidate.run_id)
                        > (current.started_at, current.run_id)
            )
        ) AS stale_current_runs,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger
            JOIN pg_class table_ref ON table_ref.oid = trigger.tgrelid
            JOIN pg_namespace namespace_ref
                ON namespace_ref.oid = table_ref.relnamespace
            WHERE namespace_ref.nspname = 'risk_platform'
              AND trigger.tgname IN (
                    'prevent_local_schedule_run_update',
                    'prevent_local_schedule_run_delete'
              )
              AND NOT trigger.tgisinternal
        ) AS append_only_trigger_count
)
SELECT
    'local_schedule_run_sessions_expand_exactly' AS check_name,
    expected_session_rows::TEXT AS expected,
    session_rows::TEXT AS actual,
    CASE WHEN expected_session_rows = session_rows THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'local_schedule_run_stages_expand_exactly',
    expected_stage_rows::TEXT,
    stage_rows::TEXT,
    CASE WHEN expected_stage_rows = stage_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'local_schedule_run_readiness_targets_exist',
    '0',
    orphan_decisions::TEXT,
    CASE WHEN orphan_decisions = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'local_schedule_run_readiness_contracts_match',
    '0',
    invalid_decision_contracts::TEXT,
    CASE WHEN invalid_decision_contracts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'local_schedule_run_override_contracts_match',
    '0',
    invalid_override_contracts::TEXT,
    CASE WHEN invalid_override_contracts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'local_schedule_run_counts_reconcile',
    '0',
    invalid_run_counts::TEXT,
    CASE WHEN invalid_run_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'local_schedule_run_session_outcomes_reconcile',
    '0',
    invalid_session_outcomes::TEXT,
    CASE WHEN invalid_session_outcomes = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'local_schedule_run_stage_outcomes_reconcile',
    '0',
    invalid_stage_outcomes::TEXT,
    CASE WHEN invalid_stage_outcomes = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'local_schedule_run_identities_unique',
    '0',
    (duplicate_run_ids + duplicate_requests)::TEXT,
    CASE WHEN duplicate_run_ids + duplicate_requests = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_local_schedule_run_grain_unique',
    '0',
    duplicate_current_schedules::TEXT,
    CASE WHEN duplicate_current_schedules = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_local_schedule_run_selects_latest',
    '0',
    stale_current_runs::TEXT,
    CASE WHEN stale_current_runs = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_local_schedule_run_row_count_matches_schedules',
    expected_current_rows::TEXT,
    current_rows::TEXT,
    CASE WHEN expected_current_rows = current_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'current_local_schedule_failures_match_current_status',
    expected_current_failure_rows::TEXT,
    current_failure_rows::TEXT,
    CASE
        WHEN expected_current_failure_rows = current_failure_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'incomplete_local_schedule_sessions_match_terminal_status',
    expected_incomplete_session_rows::TEXT,
    incomplete_session_rows::TEXT,
    CASE
        WHEN expected_incomplete_session_rows = incomplete_session_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'local_schedule_run_append_only_triggers_present',
    '2',
    append_only_trigger_count::TEXT,
    CASE WHEN append_only_trigger_count = 2 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
