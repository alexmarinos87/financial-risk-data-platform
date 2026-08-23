-- Method-aware risk-limit contract for sample and fixed-decay EWMA attribution.
-- Apply after sql/portfolio_covariance_method_schema.sql.

ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    DROP CONSTRAINT IF EXISTS
        portfolio_risk_limit_evaluations_v2_method_contract;

ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    ADD CONSTRAINT portfolio_risk_limit_evaluations_v2_method_contract
    CHECK (
        model_version <> 'portfolio-risk-limits-v2'
        OR (
            weighting_method = 'constant_weight_daily_rebalanced'
            AND (
                (
                    attribution_model_version = 'portfolio-attribution-v1'
                    AND covariance_method = 'sample_annualized'
                    AND correlation_method = 'pearson'
                )
                OR (
                    attribution_model_version =
                        'portfolio-attribution-ewma-v1'
                    AND covariance_method =
                        'ewma_zero_mean_lambda_0_94_annualized'
                    AND correlation_method =
                        'implied_from_ewma_covariance'
                )
            )
        )
    );

CREATE OR REPLACE VIEW
    risk_platform.portfolio_risk_limit_method_comparison AS
SELECT
    sample.policy_id,
    sample.portfolio_id,
    sample.base_currency,
    sample.definition_fingerprint,
    sample.ts_event AS metric_ts,
    GREATEST(sample.ts_ingest, ewma.ts_ingest) AS comparison_ts,
    sample.metric_name,
    sample.subject_type AS sample_subject_type,
    sample.subject_key AS sample_subject_key,
    ewma.subject_type AS ewma_subject_type,
    ewma.subject_key AS ewma_subject_key,
    sample.unit,
    sample.covariance_window,
    sample.annualization_days,
    sample.warning_threshold,
    sample.critical_threshold,
    sample.policy_fingerprint AS sample_policy_fingerprint,
    ewma.policy_fingerprint AS ewma_policy_fingerprint,
    sample.calculation_id AS sample_evaluation_calculation_id,
    ewma.calculation_id AS ewma_evaluation_calculation_id,
    sample.attribution_calculation_id AS sample_attribution_calculation_id,
    ewma.attribution_calculation_id AS ewma_attribution_calculation_id,
    sample.observed_value AS sample_observed_value,
    ewma.observed_value AS ewma_observed_value,
    ewma.observed_value - sample.observed_value
        AS observed_difference_ewma_minus_sample,
    ABS(ewma.observed_value - sample.observed_value)
        AS absolute_observed_difference,
    sample.status AS sample_status,
    ewma.status AS ewma_status,
    sample.is_breach AS sample_is_breach,
    ewma.is_breach AS ewma_is_breach,
    sample.status <> ewma.status AS status_disagreement,
    CASE
        WHEN sample.observed_value > ewma.observed_value THEN 'sample'
        WHEN ewma.observed_value > sample.observed_value THEN 'ewma'
        ELSE 'equal'
    END AS higher_observed_method,
    CASE
        WHEN (
            CASE sample.status
                WHEN 'critical' THEN 2
                WHEN 'warning' THEN 1
                ELSE 0
            END
        ) > (
            CASE ewma.status
                WHEN 'critical' THEN 2
                WHEN 'warning' THEN 1
                ELSE 0
            END
        ) THEN 'sample'
        WHEN (
            CASE ewma.status
                WHEN 'critical' THEN 2
                WHEN 'warning' THEN 1
                ELSE 0
            END
        ) > (
            CASE sample.status
                WHEN 'critical' THEN 2
                WHEN 'warning' THEN 1
                ELSE 0
            END
        ) THEN 'ewma'
        ELSE 'equal'
    END AS more_severe_method
FROM risk_platform.latest_portfolio_risk_limit_evaluations sample
JOIN risk_platform.latest_portfolio_risk_limit_evaluations ewma
    ON ewma.policy_id = sample.policy_id
    AND ewma.portfolio_id = sample.portfolio_id
    AND ewma.base_currency = sample.base_currency
    AND ewma.definition_fingerprint = sample.definition_fingerprint
    AND ewma.ts_event = sample.ts_event
    AND ewma.metric_name = sample.metric_name
    AND ewma.unit = sample.unit
    AND ewma.covariance_window = sample.covariance_window
    AND ewma.annualization_days = sample.annualization_days
    AND ewma.warning_threshold = sample.warning_threshold
    AND ewma.critical_threshold = sample.critical_threshold
WHERE
    sample.model_version = 'portfolio-risk-limits-v2'
    AND sample.attribution_model_version = 'portfolio-attribution-v1'
    AND sample.weighting_method = 'constant_weight_daily_rebalanced'
    AND sample.covariance_method = 'sample_annualized'
    AND sample.correlation_method = 'pearson'
    AND ewma.model_version = 'portfolio-risk-limits-v2'
    AND ewma.attribution_model_version = 'portfolio-attribution-ewma-v1'
    AND ewma.weighting_method = 'constant_weight_daily_rebalanced'
    AND ewma.covariance_method =
        'ewma_zero_mean_lambda_0_94_annualized'
    AND ewma.correlation_method =
        'implied_from_ewma_covariance';
