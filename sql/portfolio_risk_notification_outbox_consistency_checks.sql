-- Reconciliation checks for durable portfolio risk notification candidates.
-- Run after lifecycle and notification-outbox schemas are applied and local
-- outbox Parquet has been loaded.

WITH counts AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_outbox
        ) AS history_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_portfolio_risk_notification_outbox
        ) AS current_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_actionable_transitions
        ) AS actionable_transition_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_pending
        ) AS pending_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_portfolio_risk_notification_outbox
            WHERE delivery_disposition = 'pending'
        ) AS expected_pending_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_suppressed
        ) AS suppressed_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_portfolio_risk_notification_outbox
            WHERE delivery_disposition = 'suppressed'
        ) AS expected_suppressed_rows,
        (
            SELECT COALESCE(SUM(event_count), 0)
            FROM risk_platform.portfolio_risk_notification_outbox_summary
        ) AS summary_event_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_outbox outbox
            LEFT JOIN
                risk_platform.portfolio_risk_limit_evaluations evaluation
                ON evaluation.calculation_id
                    = outbox.source_evaluation_calculation_id
            WHERE evaluation.calculation_id IS NULL
        ) AS orphan_source_evaluations,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_outbox outbox
            LEFT JOIN
                risk_platform.portfolio_risk_limit_evaluations previous
                ON previous.calculation_id
                    = outbox.source_previous_evaluation_calculation_id
            WHERE
                outbox.source_previous_evaluation_calculation_id IS NOT NULL
                AND previous.calculation_id IS NULL
        ) AS orphan_previous_evaluations,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_portfolio_risk_notification_outbox outbox
            JOIN
                risk_platform.portfolio_risk_limit_actionable_transitions transition
                ON transition.calculation_id
                    = outbox.source_evaluation_calculation_id
               AND transition.transition_type = outbox.transition_type
            WHERE
                outbox.risk_limit_model_version <> transition.model_version
                OR outbox.policy_id <> transition.policy_id
                OR outbox.policy_fingerprint <> transition.policy_fingerprint
                OR outbox.portfolio_id <> transition.portfolio_id
                OR outbox.base_currency <> transition.base_currency
                OR outbox.definition_fingerprint
                    <> transition.definition_fingerprint
                OR outbox.attribution_model_version
                    <> transition.attribution_model_version
                OR outbox.weighting_method <> transition.weighting_method
                OR outbox.covariance_method <> transition.covariance_method
                OR outbox.correlation_method <> transition.correlation_method
                OR outbox.covariance_window <> transition.covariance_window
                OR outbox.annualization_days <> transition.annualization_days
                OR outbox.ts_event <> transition.ts_event
                OR outbox.ts_ingest <> transition.ts_ingest
                OR outbox.metric_name <> transition.metric_name
                OR outbox.subject_type <> transition.subject_type
                OR outbox.subject_key <> transition.subject_key
                OR outbox.previous_subject_key IS DISTINCT FROM
                    transition.previous_subject_key
                OR outbox.subject_changed <> transition.subject_changed
                OR outbox.unit <> transition.unit
                OR outbox.previous_status IS DISTINCT FROM
                    transition.previous_status
                OR outbox.current_status <> transition.status
                OR outbox.severity_rank <> transition.severity_rank
                OR ABS(outbox.observed_value - transition.observed_value)
                    > 0.0000000001
                OR ABS(
                    outbox.observed_signed_value
                    - transition.observed_signed_value
                ) > 0.0000000001
                OR ABS(
                    outbox.warning_threshold
                    - transition.warning_threshold
                ) > 0.0000000001
                OR ABS(
                    outbox.critical_threshold
                    - transition.critical_threshold
                ) > 0.0000000001
                OR ABS(outbox.breach_excess - transition.breach_excess)
                    > 0.0000000001
        ) AS mismatched_transition_metadata,
        (
            SELECT COUNT(*) - COUNT(DISTINCT event_id)
            FROM risk_platform.portfolio_risk_notification_outbox
        ) AS duplicate_event_ids,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_outbox
            WHERE
                (
                    transition_type IN ('opened', 'escalated', 'resolved')
                    AND (
                        delivery_disposition <> 'pending'
                        OR suppression_reason IS NOT NULL
                    )
                )
                OR (
                    transition_type = 'deescalated'
                    AND (
                        delivery_disposition <> 'suppressed'
                        OR suppression_reason <> 'deescalation_not_routed'
                    )
                )
        ) AS invalid_dispositions,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_outbox
            WHERE
                payload_json ->> 'event_id' <> event_id
                OR payload_json ->> 'event_type' <> event_type
                OR payload_json ->> 'transition_type' <> transition_type
                OR payload_json #>> '{policy,policy_id}' <> policy_id
                OR payload_json #>> '{policy,policy_fingerprint}'
                    <> policy_fingerprint
                OR payload_json #>> '{portfolio,portfolio_id}'
                    <> portfolio_id
                OR payload_json #>> '{portfolio,definition_fingerprint}'
                    <> definition_fingerprint
                OR payload_json #>> '{portfolio,base_currency}'
                    <> base_currency
                OR payload_json #>> '{source,evaluation_calculation_id}'
                    <> source_evaluation_calculation_id
                OR payload_json #>> '{metric,name}' <> metric_name
                OR payload_json #>> '{metric,subject_key}' <> subject_key
                OR payload_json #>> '{metric,current_status}' <> current_status
        ) AS invalid_payload_identity,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_actionable_transitions transition
            LEFT JOIN
                risk_platform.current_portfolio_risk_notification_outbox outbox
                ON outbox.source_evaluation_calculation_id
                    = transition.calculation_id
               AND outbox.transition_type = transition.transition_type
               AND (
                    outbox.source_previous_evaluation_calculation_id
                        = transition.previous_calculation_id
                    OR (
                        outbox.source_previous_evaluation_calculation_id IS NULL
                        AND transition.previous_calculation_id IS NULL
                    )
               )
            WHERE outbox.event_id IS NULL
        ) AS missing_current_candidates,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_portfolio_risk_notification_outbox outbox
            WHERE NOT EXISTS (
                SELECT 1
                FROM risk_platform.portfolio_risk_limit_actionable_transitions transition
                WHERE transition.calculation_id
                        = outbox.source_evaluation_calculation_id
                  AND transition.transition_type = outbox.transition_type
                  AND (
                        transition.previous_calculation_id
                            = outbox.source_previous_evaluation_calculation_id
                        OR (
                            transition.previous_calculation_id IS NULL
                            AND outbox.source_previous_evaluation_calculation_id
                                IS NULL
                        )
                  )
            )
        ) AS stale_current_candidates
)
SELECT
    'portfolio_notification_history_rows_present' AS check_name,
    '>=0' AS expected,
    history_rows::TEXT AS actual,
    'pass' AS status
FROM counts

UNION ALL

SELECT
    'portfolio_notification_current_rows_match_actionable_transitions',
    actionable_transition_rows::TEXT,
    current_rows::TEXT,
    CASE
        WHEN actionable_transition_rows = current_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'portfolio_notification_pending_view_matches',
    expected_pending_rows::TEXT,
    pending_rows::TEXT,
    CASE WHEN expected_pending_rows = pending_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'portfolio_notification_suppressed_view_matches',
    expected_suppressed_rows::TEXT,
    suppressed_rows::TEXT,
    CASE
        WHEN expected_suppressed_rows = suppressed_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'portfolio_notification_summary_rows_reconcile',
    current_rows::TEXT,
    summary_event_rows::TEXT,
    CASE WHEN current_rows = summary_event_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'portfolio_notification_source_evaluations_exist',
    '0',
    orphan_source_evaluations::TEXT,
    CASE WHEN orphan_source_evaluations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_previous_evaluations_exist',
    '0',
    orphan_previous_evaluations::TEXT,
    CASE WHEN orphan_previous_evaluations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_transition_metadata_matches',
    '0',
    mismatched_transition_metadata::TEXT,
    CASE
        WHEN mismatched_transition_metadata = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_event_ids_unique',
    '0',
    duplicate_event_ids::TEXT,
    CASE WHEN duplicate_event_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_dispositions_valid',
    '0',
    invalid_dispositions::TEXT,
    CASE WHEN invalid_dispositions = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_payload_identity_matches',
    '0',
    invalid_payload_identity::TEXT,
    CASE WHEN invalid_payload_identity = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_current_candidates_complete',
    '0',
    missing_current_candidates::TEXT,
    CASE WHEN missing_current_candidates = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_notification_current_candidates_not_stale',
    '0',
    stale_current_candidates::TEXT,
    CASE WHEN stale_current_candidates = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
