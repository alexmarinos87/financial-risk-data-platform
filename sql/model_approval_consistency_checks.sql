-- Reconciliation checks for append-only model approval evidence.

WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM risk_platform.model_approvals)
            AS approval_rows,
        (SELECT COUNT(*) FROM risk_platform.model_approval_revocations)
            AS revocation_rows,
        (SELECT COUNT(*) FROM risk_platform.model_approval_event_history)
            AS history_rows,
        (SELECT COUNT(*) FROM risk_platform.current_model_approval_status)
            AS current_status_rows,
        (SELECT COUNT(*) FROM risk_platform.current_model_approvals)
            AS current_approved_rows,
        (SELECT COUNT(*) FROM risk_platform.revoked_model_approvals)
            AS current_revoked_rows
), integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.model_approval_revocations revocation
            LEFT JOIN risk_platform.model_approvals approval
                ON approval.approval_id = revocation.approval_id
            WHERE approval.approval_id IS NULL
        ) AS orphan_revocations,
        (
            SELECT COUNT(*)
            FROM risk_platform.model_approval_revocations revocation
            JOIN risk_platform.model_approvals approval
                ON approval.approval_id = revocation.approval_id
            WHERE revocation.revoked_at < approval.approved_at
        ) AS invalid_revocation_times,
        (
            SELECT COUNT(*)
            FROM (
                SELECT request_id, COUNT(*) AS row_count
                FROM risk_platform.model_approvals
                GROUP BY request_id
                HAVING COUNT(*) > 1
            ) duplicate_approval_requests
        ) AS duplicate_approval_request_grains,
        (
            SELECT COUNT(*)
            FROM (
                SELECT request_id, COUNT(*) AS row_count
                FROM risk_platform.model_approval_revocations
                GROUP BY request_id
                HAVING COUNT(*) > 1
            ) duplicate_revocation_requests
        ) AS duplicate_revocation_request_grains,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    use_case,
                    contract_fingerprint,
                    COUNT(*) AS row_count
                FROM risk_platform.current_model_approval_status
                GROUP BY use_case, contract_fingerprint
                HAVING COUNT(*) > 1
            ) duplicate_current_grains
        ) AS duplicate_current_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_model_approval_status current_status
            JOIN risk_platform.model_approvals candidate
                ON candidate.use_case = current_status.use_case
                AND candidate.contract_fingerprint =
                    current_status.contract_fingerprint
            WHERE (
                candidate.approved_at,
                candidate.approval_id
            ) > (
                current_status.approved_at,
                current_status.approval_id
            )
        ) AS stale_current_approvals,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_model_approval_status current_status
            WHERE (
                current_status.approval_status = 'approved'
                AND EXISTS (
                    SELECT 1
                    FROM risk_platform.model_approval_revocations revocation
                    WHERE revocation.approval_id = current_status.approval_id
                )
            )
            OR (
                current_status.approval_status = 'revoked'
                AND NOT EXISTS (
                    SELECT 1
                    FROM risk_platform.model_approval_revocations revocation
                    WHERE revocation.approval_id = current_status.approval_id
                )
            )
        ) AS invalid_current_statuses,
        (
            SELECT COUNT(*)
            FROM risk_platform.model_approvals approval
            WHERE (
                approval.attribution_model_version = 'portfolio-attribution-v1'
                AND (
                    approval.weighting_method
                        <> 'constant_weight_daily_rebalanced'
                    OR approval.covariance_method <> 'sample_annualized'
                    OR approval.correlation_method <> 'pearson'
                    OR approval.fixed_parameters_json <> '{"annualization_days":252,"degrees_of_freedom":1,"estimator":"sample"}'::JSONB
                )
            )
            OR (
                approval.attribution_model_version =
                    'portfolio-attribution-ewma-v1'
                AND (
                    approval.weighting_method
                        <> 'constant_weight_daily_rebalanced'
                    OR approval.covariance_method
                        <> 'ewma_zero_mean_lambda_0_94_annualized'
                    OR approval.correlation_method
                        <> 'implied_from_ewma_covariance'
                    OR approval.fixed_parameters_json <> '{"annualization_days":252,"decay":0.94,"mean_assumption":"zero_daily"}'::JSONB
                )
            )
            OR approval.attribution_model_version NOT IN (
                'portfolio-attribution-v1',
                'portfolio-attribution-ewma-v1'
            )
        ) AS invalid_model_contracts,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger_record
            JOIN pg_class table_record
                ON table_record.oid = trigger_record.tgrelid
            JOIN pg_namespace namespace_record
                ON namespace_record.oid = table_record.relnamespace
            WHERE namespace_record.nspname = 'risk_platform'
              AND table_record.relname IN (
                  'model_approvals',
                  'model_approval_revocations'
              )
              AND trigger_record.tgname IN (
                  'validate_model_approval_revocation_insert',
                  'prevent_model_approval_update',
                  'prevent_model_approval_delete',
                  'prevent_model_approval_revocation_update',
                  'prevent_model_approval_revocation_delete'
              )
              AND NOT trigger_record.tgisinternal
              AND trigger_record.tgenabled <> 'D'
        ) AS enabled_governance_triggers
)
SELECT
    'model_approval_history_matches_event_tables' AS check_name,
    (approval_rows + revocation_rows)::TEXT AS expected,
    history_rows::TEXT AS actual,
    CASE
        WHEN history_rows = approval_rows + revocation_rows THEN 'pass'
        ELSE 'fail'
    END AS status
FROM counts

UNION ALL

SELECT
    'model_approval_revocations_reference_approvals',
    '0',
    orphan_revocations::TEXT,
    CASE WHEN orphan_revocations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'model_approval_revocation_times_are_valid',
    '0',
    invalid_revocation_times::TEXT,
    CASE WHEN invalid_revocation_times = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'model_approval_request_grains_are_unique',
    '0',
    duplicate_approval_request_grains::TEXT,
    CASE
        WHEN duplicate_approval_request_grains = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'model_revocation_request_grains_are_unique',
    '0',
    duplicate_revocation_request_grains::TEXT,
    CASE
        WHEN duplicate_revocation_request_grains = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'current_model_approval_grain_is_unique',
    '0',
    duplicate_current_grains::TEXT,
    CASE WHEN duplicate_current_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_model_approval_selects_latest_approval',
    '0',
    stale_current_approvals::TEXT,
    CASE WHEN stale_current_approvals = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_model_approval_status_matches_revocations',
    '0',
    invalid_current_statuses::TEXT,
    CASE WHEN invalid_current_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'model_approval_contracts_are_supported',
    '0',
    invalid_model_contracts::TEXT,
    CASE WHEN invalid_model_contracts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_model_approval_views_partition_status',
    current_status_rows::TEXT,
    (current_approved_rows + current_revoked_rows)::TEXT,
    CASE
        WHEN current_status_rows = current_approved_rows + current_revoked_rows
            THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'model_approval_append_only_triggers_enabled',
    '5',
    enabled_governance_triggers::TEXT,
    CASE WHEN enabled_governance_triggers = 5 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
