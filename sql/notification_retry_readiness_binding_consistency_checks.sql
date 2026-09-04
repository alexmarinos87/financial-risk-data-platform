-- Reconciliation for append-only retry readiness binding history.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_notification_retry_executions)
            AS terminal_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_readiness_bindings)
            AS binding_rows,
        (SELECT COUNT(DISTINCT terminal_record_id)
         FROM risk_platform.notification_retry_readiness_bindings)
            AS distinct_terminal_record_ids,
        (SELECT COUNT(DISTINCT enforcement_id)
         FROM risk_platform.notification_retry_readiness_bindings)
            AS distinct_enforcement_ids,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_readiness_binding_status)
            AS status_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_readiness_bound)
            AS bound_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_readiness_binding_missing)
            AS missing_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_bindings binding
            JOIN risk_platform.portfolio_risk_notification_retry_executions terminal
              ON terminal.record_id = binding.terminal_record_id
            WHERE binding.terminal_request_id <> terminal.request_id
               OR binding.terminal_plan_id <> terminal.plan_id
               OR binding.terminal_execution_id
                    IS DISTINCT FROM terminal.execution_id
               OR binding.terminal_status <> terminal.terminal_status
               OR binding.terminal_started_at <> terminal.started_at
               OR binding.terminal_finished_at <> terminal.finished_at
               OR binding.terminal_recorded_at <> terminal.recorded_at
               OR binding.terminal_request_count <> terminal.request_count
               OR binding.terminal_attempts_persisted
                    <> terminal.attempts_persisted
               OR binding.terminal_document_sha256
                    <> terminal.document_sha256
        ) AS invalid_terminal_source_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_bindings binding
            JOIN risk_platform.notification_execution_readiness_decisions readiness
              ON readiness.record_id = binding.readiness_record_id
            WHERE binding.readiness_request_id <> readiness.request_id
               OR binding.retained_decision_id <> readiness.decision_id
               OR binding.destination_id <> readiness.destination_id
               OR readiness.execution_kind <> 'retry'
               OR readiness.decision <> 'allow'
               OR jsonb_array_length(readiness.blocking_reasons_json) <> 0
               OR binding.retained_decision_evaluated_at
                    <> readiness.evaluated_at
        ) AS invalid_readiness_source_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_bindings binding
            WHERE binding.binding_json ->> 'binding_id' <> binding.binding_id
               OR binding.binding_json ->> 'model_version'
                    <> binding.model_version
               OR binding.binding_json -> 'terminal_execution' ->> 'record_id'
                    <> binding.terminal_record_id
               OR binding.binding_json -> 'terminal_execution' ->> 'request_id'
                    <> binding.terminal_request_id
               OR binding.binding_json -> 'terminal_execution' ->> 'plan_id'
                    <> binding.terminal_plan_id
               OR binding.binding_json -> 'terminal_execution'
                    ->> 'document_sha256' <> binding.terminal_document_sha256
               OR binding.binding_json -> 'readiness_enforcement'
                    <> binding.readiness_enforcement_json
               OR binding.binding_json ->> 'readiness_enforcement_sha256'
                    <> binding.readiness_enforcement_sha256
               OR (binding.binding_json ->> 'recorded_at')::TIMESTAMPTZ
                    <> binding.binding_recorded_at
        ) AS invalid_binding_document_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_bindings binding
            WHERE binding.execution_kind <> 'retry'
               OR binding.refreshed_decision_evaluated_at
                    <> binding.enforced_at
               OR binding.retained_decision_evaluated_at
                    > binding.enforced_at
               OR binding.terminal_started_at > binding.enforced_at
               OR binding.enforced_at > binding.terminal_finished_at
               OR binding.terminal_recorded_at > binding.binding_recorded_at
               OR NOT (
                    binding.readiness_enforcement_json
                        ->> 'execution_ready'
               )::BOOLEAN
               OR binding.readiness_enforcement_json
                    ->> 'readiness_review_status' <> 'allowed'
               OR NOT (
                    binding.readiness_enforcement_json
                        ->> 'substantive_evidence_match'
               )::BOOLEAN
        ) AS invalid_authority_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT terminal_record_id
                FROM risk_platform.notification_retry_readiness_binding_status
                GROUP BY terminal_record_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_status_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_binding_status
            WHERE readiness_binding_status NOT IN ('bound', 'binding_missing')
               OR readiness_binding_review_required
                    <> (readiness_binding_status = 'binding_missing')
               OR (readiness_binding_status = 'bound' AND binding_id IS NULL)
               OR (
                    readiness_binding_status = 'binding_missing'
                    AND binding_id IS NOT NULL
               )
        ) AS invalid_status_rows,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger
            JOIN pg_class relation ON relation.oid = trigger.tgrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'risk_platform'
              AND relation.relname = 'notification_retry_readiness_bindings'
              AND trigger.tgname IN (
                  'notification_retry_readiness_binding_reject_update',
                  'notification_retry_readiness_binding_reject_delete'
              )
              AND trigger.tgenabled <> 'D'
              AND NOT trigger.tgisinternal
        ) AS append_only_trigger_count
)
SELECT
    'notification_retry_readiness_terminal_ids_unique' AS check_name,
    binding_rows::TEXT AS expected,
    distinct_terminal_record_ids::TEXT AS actual,
    CASE
        WHEN binding_rows = distinct_terminal_record_ids THEN 'pass'
        ELSE 'fail'
    END AS status
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_enforcement_ids_unique',
    binding_rows::TEXT,
    distinct_enforcement_ids::TEXT,
    CASE
        WHEN binding_rows = distinct_enforcement_ids THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_status_covers_terminal_history',
    terminal_rows::TEXT,
    status_rows::TEXT,
    CASE WHEN terminal_rows = status_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_bound_partition_matches',
    binding_rows::TEXT,
    bound_rows::TEXT,
    CASE WHEN binding_rows = bound_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_missing_partition_matches',
    (terminal_rows - binding_rows)::TEXT,
    missing_rows::TEXT,
    CASE
        WHEN terminal_rows - binding_rows = missing_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_terminal_sources_reconcile',
    '0',
    invalid_terminal_source_rows::TEXT,
    CASE WHEN invalid_terminal_source_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_decision_sources_reconcile',
    '0',
    invalid_readiness_source_rows::TEXT,
    CASE WHEN invalid_readiness_source_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_documents_reconcile',
    '0',
    invalid_binding_document_rows::TEXT,
    CASE WHEN invalid_binding_document_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_authority_is_valid',
    '0',
    invalid_authority_rows::TEXT,
    CASE WHEN invalid_authority_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_status_grain_is_unique',
    '0',
    duplicate_status_grains::TEXT,
    CASE WHEN duplicate_status_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_status_flags_are_valid',
    '0',
    invalid_status_rows::TEXT,
    CASE WHEN invalid_status_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_history_is_append_only',
    '2',
    append_only_trigger_count::TEXT,
    CASE WHEN append_only_trigger_count = 2 THEN 'pass' ELSE 'fail' END
FROM integrity;
