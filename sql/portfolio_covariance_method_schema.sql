-- PostgreSQL contract for aligned sample-versus-EWMA portfolio covariance serving.
-- Apply after sql/portfolio_attribution_schema.sql.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE
            conname = 'chk_portfolio_attribution_ewma_v1'
            AND conrelid = 'risk_platform.portfolio_risk_attribution'::regclass
    ) THEN
        ALTER TABLE risk_platform.portfolio_risk_attribution
            ADD CONSTRAINT chk_portfolio_attribution_ewma_v1 CHECK (
                model_version <> 'portfolio-attribution-ewma-v1'
                OR (
                    weighting_method = 'constant_weight_daily_rebalanced'
                    AND covariance_method
                        = 'ewma_zero_mean_lambda_0_94_annualized'
                    AND correlation_method
                        = 'implied_from_ewma_covariance'
                    AND annualization_days = 252
                )
            );
    END IF;
END
$$;

CREATE OR REPLACE VIEW risk_platform.portfolio_covariance_method_comparison AS
SELECT
    sample.portfolio_id,
    sample.base_currency,
    sample.definition_fingerprint,
    sample.weighting_method,
    sample.covariance_window,
    sample.window_start,
    sample.window_end,
    sample.window_observations,
    sample.annualization_days,
    sample.ts_event AS metric_ts,
    sample.input_calculation_ids_json,
    sample.calculation_id AS sample_calculation_id,
    sample.ts_ingest AS sample_calculation_ts,
    sample.portfolio_variance_annualized
        AS sample_portfolio_variance_annualized,
    sample.portfolio_volatility_annualized
        AS sample_portfolio_volatility_annualized,
    ewma.calculation_id AS ewma_calculation_id,
    ewma.ts_ingest AS ewma_calculation_ts,
    ewma.portfolio_variance_annualized
        AS ewma_portfolio_variance_annualized,
    ewma.portfolio_volatility_annualized
        AS ewma_portfolio_volatility_annualized,
    ewma.portfolio_volatility_annualized
        - sample.portfolio_volatility_annualized
        AS ewma_minus_sample_volatility,
    CASE
        WHEN sample.portfolio_volatility_annualized > 0
        THEN ewma.portfolio_volatility_annualized
            / sample.portfolio_volatility_annualized
        ELSE NULL
    END AS ewma_to_sample_volatility_ratio,
    CASE
        WHEN ewma.portfolio_volatility_annualized
            > sample.portfolio_volatility_annualized
        THEN 'ewma'
        WHEN ewma.portfolio_volatility_annualized
            < sample.portfolio_volatility_annualized
        THEN 'sample'
        ELSE 'equal'
    END AS higher_volatility_model
FROM risk_platform.latest_portfolio_risk_attribution sample
JOIN risk_platform.latest_portfolio_risk_attribution ewma
    ON ewma.portfolio_id = sample.portfolio_id
    AND ewma.base_currency = sample.base_currency
    AND ewma.definition_fingerprint = sample.definition_fingerprint
    AND ewma.weighting_method = sample.weighting_method
    AND ewma.covariance_window = sample.covariance_window
    AND ewma.window_start = sample.window_start
    AND ewma.window_end = sample.window_end
    AND ewma.window_observations = sample.window_observations
    AND ewma.annualization_days = sample.annualization_days
    AND ewma.ts_event = sample.ts_event
    AND ewma.input_calculation_ids_json = sample.input_calculation_ids_json
WHERE
    sample.model_version = 'portfolio-attribution-v1'
    AND sample.covariance_method = 'sample_annualized'
    AND sample.correlation_method = 'pearson'
    AND ewma.model_version = 'portfolio-attribution-ewma-v1'
    AND ewma.covariance_method = 'ewma_zero_mean_lambda_0_94_annualized'
    AND ewma.correlation_method = 'implied_from_ewma_covariance';
