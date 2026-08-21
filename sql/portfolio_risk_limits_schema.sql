-- PostgreSQL serving contract for deterministic portfolio risk-limit evaluations.
-- Apply after sql/portfolio_attribution_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.portfolio_risk_limit_evaluations (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    base_currency CHAR(3) NOT NULL,
    definition_fingerprint TEXT NOT NULL,
    attribution_calculation_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_risk_attribution (calculation_id),
    attribution_model_version TEXT NOT NULL,
    weighting_method TEXT NOT NULL,
    covariance_method TEXT NOT NULL,
    correlation_method TEXT NOT NULL,
    covariance_window INTEGER NOT NULL CHECK (
        covariance_window >= 2 AND covariance_window <= 2520
    ),
    annualization_days INTEGER NOT NULL CHECK (
        annualization_days > 0 AND annualization_days <= 2520
    ),
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL CHECK (
        metric_name IN (
            'portfolio_volatility_annualized',
            'largest_absolute_component_contribution_share'
        )
    ),
    subject_type TEXT NOT NULL CHECK (
        subject_type IN ('portfolio', 'constituent')
    ),
    subject_key TEXT NOT NULL,
    unit TEXT NOT NULL CHECK (
        unit IN ('annualized_decimal', 'absolute_share')
    ),
    observed_value DOUBLE PRECISION NOT NULL,
    observed_signed_value DOUBLE PRECISION NOT NULL,
    warning_threshold DOUBLE PRECISION NOT NULL,
    critical_threshold DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'warning', 'critical')),
    is_breach BOOLEAN NOT NULL,
    breach_threshold DOUBLE PRECISION,
    breach_excess DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (policy_id <> ''),
    CHECK (policy_fingerprint <> ''),
    CHECK (portfolio_id <> ''),
    CHECK (base_currency = UPPER(base_currency)),
    CHECK (definition_fingerprint <> ''),
    CHECK (attribution_calculation_id <> ''),
    CHECK (attribution_model_version <> ''),
    CHECK (weighting_method <> ''),
    CHECK (covariance_method <> ''),
    CHECK (correlation_method <> ''),
    CHECK (subject_key <> ''),
    CHECK (ts_ingest >= ts_event),
    CHECK (
        observed_value >= 0
        AND observed_value < 'Infinity'::DOUBLE PRECISION
        AND observed_value <> 'NaN'::DOUBLE PRECISION
    ),
    CHECK (
        observed_signed_value > '-Infinity'::DOUBLE PRECISION
        AND observed_signed_value < 'Infinity'::DOUBLE PRECISION
        AND observed_signed_value <> 'NaN'::DOUBLE PRECISION
    ),
    CHECK (
        warning_threshold > 0
        AND critical_threshold > warning_threshold
        AND critical_threshold < 'Infinity'::DOUBLE PRECISION
    ),
    CHECK (
        breach_excess >= 0
        AND breach_excess < 'Infinity'::DOUBLE PRECISION
    ),
    CHECK (
        (status = 'ok'
            AND NOT is_breach
            AND breach_threshold IS NULL
            AND breach_excess = 0
            AND observed_value < warning_threshold)
        OR (status = 'warning'
            AND is_breach
            AND breach_threshold = warning_threshold
            AND observed_value >= warning_threshold
            AND observed_value < critical_threshold
            AND ABS(breach_excess - (observed_value - warning_threshold))
                <= 0.0000000001)
        OR (status = 'critical'
            AND is_breach
            AND breach_threshold = critical_threshold
            AND observed_value >= critical_threshold
            AND ABS(breach_excess - (observed_value - critical_threshold))
                <= 0.0000000001)
    ),
    CHECK (
        (metric_name = 'portfolio_volatility_annualized'
            AND subject_type = 'portfolio'
            AND subject_key = portfolio_id
            AND unit = 'annualized_decimal'
            AND observed_signed_value = observed_value)
        OR (metric_name = 'largest_absolute_component_contribution_share'
            AND subject_type = 'constituent'
            AND unit = 'absolute_share'
            AND ABS(ABS(observed_signed_value) - observed_value)
                <= 0.0000000001)
    ),
    CHECK (
        model_version <> 'portfolio-risk-limits-v1'
        OR (
            attribution_model_version = 'portfolio-attribution-v1'
            AND weighting_method = 'constant_weight_daily_rebalanced'
            AND covariance_method = 'sample_annualized'
            AND correlation_method = 'pearson'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_portfolio_risk_limits_version_lookup
    ON risk_platform.portfolio_risk_limit_evaluations (
        policy_id,
        policy_fingerprint,
        portfolio_id,
        definition_fingerprint,
        ts_event DESC,
        model_version,
        attribution_model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        covariance_window,
        annualization_days,
        metric_name,
        ts_ingest DESC,
        calculation_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_portfolio_risk_limits_breaches
    ON risk_platform.portfolio_risk_limit_evaluations (
        policy_id,
        portfolio_id,
        ts_event DESC,
        status
    )
    WHERE is_breach;

CREATE OR REPLACE VIEW risk_platform.latest_portfolio_risk_limit_evaluations AS
SELECT
    calculation_id,
    model_version,
    policy_id,
    policy_fingerprint,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    attribution_calculation_id,
    attribution_model_version,
    weighting_method,
    covariance_method,
    correlation_method,
    covariance_window,
    annualization_days,
    ts_event,
    ts_ingest,
    metric_name,
    subject_type,
    subject_key,
    unit,
    observed_value,
    observed_signed_value,
    warning_threshold,
    critical_threshold,
    status,
    is_breach,
    breach_threshold,
    breach_excess
FROM (
    SELECT
        evaluation.*,
        ROW_NUMBER() OVER (
            PARTITION BY
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
            ORDER BY ts_ingest DESC, calculation_id DESC
        ) AS version_rank
    FROM risk_platform.portfolio_risk_limit_evaluations evaluation
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_breaches AS
SELECT *
FROM risk_platform.latest_portfolio_risk_limit_evaluations
WHERE is_breach;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_snapshot_status AS
SELECT
    policy_id,
    policy_fingerprint,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    attribution_calculation_id,
    attribution_model_version,
    weighting_method,
    covariance_method,
    correlation_method,
    covariance_window,
    annualization_days,
    ts_event AS metric_ts,
    MAX(ts_ingest) AS calculation_ts,
    COUNT(*) AS metric_count,
    COUNT(*) FILTER (WHERE is_breach) AS breach_count,
    CASE MAX(
        CASE status WHEN 'critical' THEN 2 WHEN 'warning' THEN 1 ELSE 0 END
    )
        WHEN 2 THEN 'critical'
        WHEN 1 THEN 'warning'
        ELSE 'ok'
    END AS overall_status
FROM risk_platform.latest_portfolio_risk_limit_evaluations
GROUP BY
    policy_id,
    policy_fingerprint,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    attribution_calculation_id,
    attribution_model_version,
    weighting_method,
    covariance_method,
    correlation_method,
    covariance_window,
    annualization_days,
    ts_event;
