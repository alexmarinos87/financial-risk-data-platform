-- Reconciliation checks for portfolio risk-limit monitoring and human acknowledgements.
-- Run after attribution and limit outputs have been loaded into PostgreSQL.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_evaluations) AS history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_portfolio_risk_limit_evaluations) AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_breaches) AS breach_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_portfolio_risk_limit_evaluations
         WHERE is_breach) AS expected_breach_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_snapshot_status) AS status_rows,
        (SELECT COUNT(*) FROM (
            SELECT DISTINCT
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                attribution_calculation_id,
                ts_event
            FROM risk_platform.latest_portfolio_risk_limit_evaluations
        ) snapshots) AS expected_status_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_acknowledgements)
            AS acknowledgement_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_acknowledgement_history)
            AS acknowledgement_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_breach_status)
            AS breach_status_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_open_breaches)
            AS open_breach_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_acknowledged_breaches)
            AS acknowledged_breach_rows
),
integrity AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_evaluations evaluation
         LEFT JOIN risk_platform.portfolio_risk_attribution attribution
           ON attribution.calculation_id = evaluation.attribution_calculation_id
         WHERE attribution.calculation_id IS NULL) AS orphan_attributions,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_evaluations evaluation
         JOIN risk_platform.portfolio_risk_attribution attribution
           ON attribution.calculation_id = evaluation.attribution_calculation_id
         WHERE evaluation.portfolio_id <> attribution.portfolio_id
            OR evaluation.base_currency <> attribution.base_currency
            OR evaluation.definition_fingerprint
                <> attribution.definition_fingerprint
            OR evaluation.attribution_model_version <> attribution.model_version
            OR evaluation.weighting_method <> attribution.weighting_method
            OR evaluation.covariance_method <> attribution.covariance_method
            OR evaluation.correlation_method <> attribution.correlation_method
            OR evaluation.covariance_window <> attribution.covariance_window
            OR evaluation.annualization_days <> attribution.annualization_days
            OR evaluation.ts_event <> attribution.ts_event
            OR evaluation.ts_ingest <> attribution.ts_ingest)
            AS mismatched_attribution_metadata,
        (SELECT COUNT(*)
         FROM risk_platform.latest_portfolio_risk_limit_evaluations evaluation
         LEFT JOIN risk_platform.latest_portfolio_risk_attribution attribution
           ON attribution.calculation_id = evaluation.attribution_calculation_id
         WHERE attribution.calculation_id IS NULL)
            AS stale_attribution_references,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_evaluations evaluation
         JOIN risk_platform.portfolio_risk_attribution attribution
           ON attribution.calculation_id = evaluation.attribution_calculation_id
         WHERE
            (evaluation.metric_name = 'portfolio_volatility_annualized'
             AND ABS(
                 evaluation.observed_value
                 - attribution.portfolio_volatility_annualized
             ) > 0.0000000001)
            OR
            (evaluation.metric_name
                = 'largest_absolute_component_contribution_share'
             AND ABS(
                 evaluation.observed_value
                 - (
                    SELECT MAX(ABS(component.value::DOUBLE PRECISION))
                    FROM jsonb_each_text(
                        attribution.component_contribution_share_json
                    ) component
                 )
             ) > 0.0000000001)
            OR
            (evaluation.metric_name
                = 'largest_absolute_component_contribution_share'
             AND ABS(
                 evaluation.observed_signed_value
                 - (
                    attribution.component_contribution_share_json
                    ->> evaluation.subject_key
                 )::DOUBLE PRECISION
             ) > 0.0000000001)) AS mismatched_observed_values,
        (SELECT COUNT(*) - COUNT(DISTINCT calculation_id)
         FROM risk_platform.portfolio_risk_limit_evaluations)
            AS duplicate_calculation_ids,
        (SELECT COUNT(*) FROM (
            SELECT
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                ts_event,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name,
                COUNT(*) AS row_count
            FROM risk_platform.latest_portfolio_risk_limit_evaluations
            GROUP BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                ts_event,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name
            HAVING COUNT(*) <> 1
        ) duplicate_grains) AS duplicate_latest_grains,
        (SELECT COUNT(*)
         FROM risk_platform.latest_portfolio_risk_limit_evaluations latest
         WHERE EXISTS (
            SELECT 1
            FROM risk_platform.portfolio_risk_limit_evaluations candidate
            WHERE candidate.policy_id = latest.policy_id
              AND candidate.policy_fingerprint = latest.policy_fingerprint
              AND candidate.portfolio_id = latest.portfolio_id
              AND candidate.definition_fingerprint
                    = latest.definition_fingerprint
              AND candidate.ts_event = latest.ts_event
              AND candidate.model_version = latest.model_version
              AND candidate.attribution_model_version
                    = latest.attribution_model_version
              AND candidate.weighting_method = latest.weighting_method
              AND candidate.covariance_method = latest.covariance_method
              AND candidate.correlation_method = latest.correlation_method
              AND candidate.covariance_window = latest.covariance_window
              AND candidate.annualization_days = latest.annualization_days
              AND candidate.metric_name = latest.metric_name
              AND (candidate.ts_ingest, candidate.calculation_id)
                    > (latest.ts_ingest, latest.calculation_id)
         )) AS stale_latest_rows,
        (SELECT COUNT(*) FROM (
            SELECT
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                attribution_calculation_id,
                ts_event,
                COUNT(*) AS metric_count,
                COUNT(DISTINCT metric_name) AS distinct_metric_count
            FROM risk_platform.latest_portfolio_risk_limit_evaluations
            GROUP BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                attribution_calculation_id,
                ts_event
            HAVING COUNT(*) <> 2 OR COUNT(DISTINCT metric_name) <> 2
        ) invalid_snapshots) AS invalid_snapshot_metric_counts,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_snapshot_status status_row
         JOIN LATERAL (
            SELECT
                COUNT(*) AS metric_count,
                COUNT(*) FILTER (WHERE is_breach) AS breach_count,
                CASE MAX(
                    CASE status
                        WHEN 'critical' THEN 2
                        WHEN 'warning' THEN 1
                        ELSE 0
                    END
                )
                    WHEN 2 THEN 'critical'
                    WHEN 1 THEN 'warning'
                    ELSE 'ok'
                END AS overall_status
            FROM risk_platform.latest_portfolio_risk_limit_evaluations evaluation
            WHERE evaluation.policy_id = status_row.policy_id
              AND evaluation.policy_fingerprint = status_row.policy_fingerprint
              AND evaluation.portfolio_id = status_row.portfolio_id
              AND evaluation.definition_fingerprint
                    = status_row.definition_fingerprint
              AND evaluation.attribution_calculation_id
                    = status_row.attribution_calculation_id
              AND evaluation.ts_event = status_row.metric_ts
         ) expected ON TRUE
         WHERE status_row.metric_count <> expected.metric_count
            OR status_row.breach_count <> expected.breach_count
            OR status_row.overall_status <> expected.overall_status)
            AS invalid_snapshot_status_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
         LEFT JOIN risk_platform.portfolio_risk_limit_evaluations evaluation
           ON evaluation.calculation_id
                = acknowledgement.evaluation_calculation_id
         WHERE evaluation.calculation_id IS NULL)
            AS orphan_acknowledgements,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
         JOIN risk_platform.portfolio_risk_limit_evaluations evaluation
           ON evaluation.calculation_id
                = acknowledgement.evaluation_calculation_id
         WHERE
            NOT evaluation.is_breach
            OR acknowledgement.acknowledged_at < evaluation.ts_event)
            AS invalid_acknowledgement_targets,
        (SELECT COUNT(*)
         FROM (
            SELECT
                evaluation_calculation_id,
                request_id,
                COUNT(*) AS row_count
            FROM risk_platform.portfolio_risk_limit_acknowledgements
            GROUP BY evaluation_calculation_id, request_id
            HAVING COUNT(*) > 1
         ) duplicate_requests)
            AS duplicate_acknowledgement_requests,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_limit_breach_status breach_status
         WHERE
            breach_status.acknowledgement_count <> (
                SELECT COUNT(*)
                FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
                WHERE acknowledgement.evaluation_calculation_id
                    = breach_status.calculation_id
            )
            OR breach_status.acknowledgement_id IS DISTINCT FROM (
                SELECT acknowledgement.acknowledgement_id
                FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
                WHERE acknowledgement.evaluation_calculation_id
                    = breach_status.calculation_id
                ORDER BY
                    acknowledgement.acknowledged_at DESC,
                    acknowledgement.acknowledgement_id DESC
                LIMIT 1
            )
            OR breach_status.acknowledgement_status <> CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
                    WHERE acknowledgement.evaluation_calculation_id
                        = breach_status.calculation_id
                ) THEN 'acknowledged'
                ELSE 'unacknowledged'
            END)
            AS invalid_breach_status_rows,
        (SELECT COUNT(*)
         FROM pg_trigger
         WHERE
            tgrelid = (
                'risk_platform.portfolio_risk_limit_acknowledgements'
            )::REGCLASS
            AND NOT tgisinternal
            AND tgenabled <> 'D'
            AND tgname IN (
                'validate_risk_limit_acknowledgement_insert',
                'prevent_risk_limit_acknowledgement_update',
                'prevent_risk_limit_acknowledgement_delete'
            )) AS enabled_control_triggers
)
SELECT
    'portfolio_risk_limit_rows_present' AS check_name,
    '>=0' AS expected,
    history_rows::TEXT AS actual,
    'pass' AS status
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_attribution_references_valid', '0',
       orphan_attributions::TEXT,
       CASE WHEN orphan_attributions = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_attribution_metadata_aligns', '0',
       mismatched_attribution_metadata::TEXT,
       CASE WHEN mismatched_attribution_metadata = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_uses_current_attribution_versions', '0',
       stale_attribution_references::TEXT,
       CASE WHEN stale_attribution_references = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_observed_values_match_attribution', '0',
       mismatched_observed_values::TEXT,
       CASE WHEN mismatched_observed_values = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_calculation_ids_unique', '0',
       duplicate_calculation_ids::TEXT,
       CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'latest_portfolio_risk_limit_grain_unique', '0',
       duplicate_latest_grains::TEXT,
       CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'latest_portfolio_risk_limit_selects_current_version', '0',
       stale_latest_rows::TEXT,
       CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_snapshot_has_two_metrics', '0',
       invalid_snapshot_metric_counts::TEXT,
       CASE WHEN invalid_snapshot_metric_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_breach_view_matches_current_breaches',
       expected_breach_rows::TEXT, breach_rows::TEXT,
       CASE WHEN expected_breach_rows = breach_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_snapshot_status_rows_match_current_snapshots',
       expected_status_rows::TEXT, status_rows::TEXT,
       CASE WHEN expected_status_rows = status_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_snapshot_status_values_reconcile', '0',
       invalid_snapshot_status_rows::TEXT,
       CASE WHEN invalid_snapshot_status_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_acknowledgement_rows_valid', '>=0',
       acknowledgement_rows::TEXT, 'pass'
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_acknowledgements_reference_breaches', '0',
       orphan_acknowledgements::TEXT,
       CASE WHEN orphan_acknowledgements = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_acknowledgement_targets_are_valid', '0',
       invalid_acknowledgement_targets::TEXT,
       CASE
           WHEN invalid_acknowledgement_targets = 0 THEN 'pass'
           ELSE 'fail'
       END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_acknowledgement_requests_unique', '0',
       duplicate_acknowledgement_requests::TEXT,
       CASE
           WHEN duplicate_acknowledgement_requests = 0 THEN 'pass'
           ELSE 'fail'
       END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_acknowledgement_history_matches_base',
       acknowledgement_rows::TEXT, acknowledgement_history_rows::TEXT,
       CASE
           WHEN acknowledgement_rows = acknowledgement_history_rows THEN 'pass'
           ELSE 'fail'
       END
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_breach_status_matches_current_breaches',
       breach_rows::TEXT, breach_status_rows::TEXT,
       CASE WHEN breach_rows = breach_status_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_open_and_acknowledged_partition_breaches',
       breach_rows::TEXT,
       (open_breach_rows + acknowledged_breach_rows)::TEXT,
       CASE
           WHEN breach_rows = open_breach_rows + acknowledged_breach_rows
               THEN 'pass'
           ELSE 'fail'
       END
FROM counts

UNION ALL
SELECT 'portfolio_risk_limit_breach_status_selects_latest_acknowledgement', '0',
       invalid_breach_status_rows::TEXT,
       CASE WHEN invalid_breach_status_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL
SELECT 'portfolio_risk_limit_acknowledgement_triggers_enabled', '3',
       enabled_control_triggers::TEXT,
       CASE WHEN enabled_control_triggers = 3 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
