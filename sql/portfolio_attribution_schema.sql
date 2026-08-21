-- PostgreSQL serving contract for versioned portfolio covariance and volatility attribution.
-- Apply after sql/portfolio_schema.sql so retained portfolio-return calculations exist.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.portfolio_risk_attribution (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    base_currency CHAR(3) NOT NULL,
    definition_fingerprint TEXT NOT NULL,
    weighting_method TEXT NOT NULL,
    covariance_method TEXT NOT NULL,
    correlation_method TEXT NOT NULL,
    covariance_window INTEGER NOT NULL CHECK (
        covariance_window >= 2 AND covariance_window <= 2520
    ),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    window_observations INTEGER NOT NULL CHECK (window_observations >= 2),
    annualization_days INTEGER NOT NULL CHECK (annualization_days > 0),
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    constituent_count INTEGER NOT NULL CHECK (
        constituent_count >= 2 AND constituent_count <= 50
    ),
    weights_json JSONB NOT NULL,
    input_calculation_ids_json JSONB NOT NULL,
    input_first_calculation_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_daily_returns (calculation_id),
    input_last_calculation_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_daily_returns (calculation_id),
    covariance_annualized_json JSONB NOT NULL,
    correlation_json JSONB NOT NULL,
    constituent_volatility_annualized_json JSONB NOT NULL,
    marginal_volatility_contribution_json JSONB NOT NULL,
    component_volatility_contribution_json JSONB NOT NULL,
    component_contribution_share_json JSONB NOT NULL,
    portfolio_variance_annualized DOUBLE PRECISION NOT NULL CHECK (
        portfolio_variance_annualized >= 0
        AND portfolio_variance_annualized < 'Infinity'::DOUBLE PRECISION
    ),
    portfolio_volatility_annualized DOUBLE PRECISION NOT NULL CHECK (
        portfolio_volatility_annualized >= 0
        AND portfolio_volatility_annualized < 'Infinity'::DOUBLE PRECISION
    ),
    volatility_status TEXT NOT NULL CHECK (
        volatility_status IN ('positive', 'zero')
    ),
    correlation_status TEXT NOT NULL CHECK (
        correlation_status IN ('complete', 'undefined_zero_variance')
    ),
    undefined_correlation_cells INTEGER NOT NULL CHECK (
        undefined_correlation_cells >= 0
    ),
    euler_residual DOUBLE PRECISION NOT NULL CHECK (
        euler_residual > '-Infinity'::DOUBLE PRECISION
        AND euler_residual < 'Infinity'::DOUBLE PRECISION
        AND ABS(euler_residual) <= 0.00000001
    ),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (portfolio_id <> ''),
    CHECK (base_currency = UPPER(base_currency)),
    CHECK (definition_fingerprint <> ''),
    CHECK (weighting_method <> ''),
    CHECK (covariance_method <> ''),
    CHECK (correlation_method <> ''),
    CHECK (input_first_calculation_id <> ''),
    CHECK (input_last_calculation_id <> ''),
    CHECK (window_start < window_end),
    CHECK (window_end = ts_event),
    CHECK (ts_ingest >= ts_event),
    CHECK (window_observations = covariance_window),
    CHECK (jsonb_typeof(weights_json) = 'object'),
    CHECK (jsonb_typeof(input_calculation_ids_json) = 'array'),
    CHECK (
        jsonb_array_length(input_calculation_ids_json) = window_observations
    ),
    CHECK (jsonb_typeof(covariance_annualized_json) = 'object'),
    CHECK (jsonb_typeof(correlation_json) = 'object'),
    CHECK (
        jsonb_typeof(constituent_volatility_annualized_json) = 'object'
    ),
    CHECK (
        jsonb_typeof(marginal_volatility_contribution_json) = 'object'
    ),
    CHECK (
        jsonb_typeof(component_volatility_contribution_json) = 'object'
    ),
    CHECK (jsonb_typeof(component_contribution_share_json) = 'object'),
    CHECK (
        undefined_correlation_cells <= constituent_count * constituent_count
    ),
    CHECK (
        (correlation_status = 'complete' AND undefined_correlation_cells = 0)
        OR (
            correlation_status = 'undefined_zero_variance'
            AND undefined_correlation_cells > 0
        )
    ),
    CHECK (
        (volatility_status = 'zero'
            AND portfolio_variance_annualized = 0
            AND portfolio_volatility_annualized = 0)
        OR (
            volatility_status = 'positive'
            AND portfolio_variance_annualized > 0
            AND portfolio_volatility_annualized > 0
        )
    ),
    CHECK (
        model_version <> 'portfolio-attribution-v1'
        OR (
            weighting_method = 'constant_weight_daily_rebalanced'
            AND covariance_method = 'sample_annualized'
            AND correlation_method = 'pearson'
            AND annualization_days = 252
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_portfolio_attribution_version_lookup
    ON risk_platform.portfolio_risk_attribution (
        portfolio_id,
        definition_fingerprint,
        ts_event DESC,
        model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        covariance_window,
        annualization_days,
        ts_ingest DESC,
        calculation_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_portfolio_attribution_inputs
    ON risk_platform.portfolio_risk_attribution
    USING GIN (input_calculation_ids_json);

CREATE OR REPLACE VIEW risk_platform.latest_portfolio_risk_attribution AS
SELECT
    calculation_id,
    model_version,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    weighting_method,
    covariance_method,
    correlation_method,
    covariance_window,
    window_start,
    window_end,
    window_observations,
    annualization_days,
    ts_event,
    ts_ingest,
    constituent_count,
    weights_json,
    input_calculation_ids_json,
    input_first_calculation_id,
    input_last_calculation_id,
    covariance_annualized_json,
    correlation_json,
    constituent_volatility_annualized_json,
    marginal_volatility_contribution_json,
    component_volatility_contribution_json,
    component_contribution_share_json,
    portfolio_variance_annualized,
    portfolio_volatility_annualized,
    volatility_status,
    correlation_status,
    undefined_correlation_cells,
    euler_residual
FROM (
    SELECT
        attribution.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                portfolio_id,
                definition_fingerprint,
                ts_event,
                model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days
            ORDER BY ts_ingest DESC, calculation_id DESC
        ) AS version_rank
    FROM risk_platform.portfolio_risk_attribution attribution
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.portfolio_attribution_semantic_model AS
SELECT
    calculation_id,
    model_version,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    weighting_method,
    covariance_method,
    correlation_method,
    covariance_window,
    window_start,
    window_end,
    window_observations,
    annualization_days,
    ts_event AS metric_ts,
    ts_ingest AS calculation_ts,
    constituent_count,
    weights_json,
    input_calculation_ids_json,
    covariance_annualized_json,
    correlation_json,
    constituent_volatility_annualized_json,
    marginal_volatility_contribution_json,
    component_volatility_contribution_json,
    component_contribution_share_json,
    portfolio_variance_annualized,
    portfolio_volatility_annualized,
    volatility_status,
    correlation_status,
    undefined_correlation_cells,
    euler_residual
FROM risk_platform.latest_portfolio_risk_attribution;

CREATE OR REPLACE VIEW risk_platform.portfolio_covariance_model AS
SELECT
    attribution.calculation_id AS attribution_calculation_id,
    attribution.model_version,
    attribution.portfolio_id,
    attribution.base_currency,
    attribution.definition_fingerprint,
    attribution.weighting_method,
    attribution.covariance_method,
    attribution.covariance_window,
    attribution.annualization_days,
    attribution.window_start,
    attribution.window_end,
    attribution.ts_event AS metric_ts,
    matrix_row.key AS row_constituent_key,
    matrix_cell.key AS column_constituent_key,
    matrix_cell.value::TEXT::DOUBLE PRECISION AS covariance_annualized
FROM risk_platform.latest_portfolio_risk_attribution attribution
CROSS JOIN LATERAL jsonb_each(
    attribution.covariance_annualized_json
) matrix_row
CROSS JOIN LATERAL jsonb_each(
    matrix_row.value
) matrix_cell;

CREATE OR REPLACE VIEW risk_platform.portfolio_correlation_model AS
SELECT
    attribution.calculation_id AS attribution_calculation_id,
    attribution.model_version,
    attribution.portfolio_id,
    attribution.base_currency,
    attribution.definition_fingerprint,
    attribution.weighting_method,
    attribution.correlation_method,
    attribution.covariance_window,
    attribution.window_start,
    attribution.window_end,
    attribution.ts_event AS metric_ts,
    matrix_row.key AS row_constituent_key,
    matrix_cell.key AS column_constituent_key,
    NULLIF(matrix_cell.value::TEXT, 'null')::DOUBLE PRECISION AS correlation,
    matrix_cell.value = 'null'::JSONB AS correlation_undefined
FROM risk_platform.latest_portfolio_risk_attribution attribution
CROSS JOIN LATERAL jsonb_each(
    attribution.correlation_json
) matrix_row
CROSS JOIN LATERAL jsonb_each(
    matrix_row.value
) matrix_cell;

CREATE OR REPLACE VIEW risk_platform.portfolio_volatility_contribution_model AS
SELECT
    attribution.calculation_id AS attribution_calculation_id,
    attribution.model_version,
    attribution.portfolio_id,
    attribution.base_currency,
    attribution.definition_fingerprint,
    attribution.weighting_method,
    attribution.covariance_method,
    attribution.covariance_window,
    attribution.annualization_days,
    attribution.window_start,
    attribution.window_end,
    attribution.ts_event AS metric_ts,
    component.key AS constituent_key,
    (attribution.weights_json ->> component.key)::DOUBLE PRECISION AS weight,
    (
        attribution.constituent_volatility_annualized_json
        ->> component.key
    )::DOUBLE PRECISION AS constituent_volatility_annualized,
    (
        attribution.marginal_volatility_contribution_json
        ->> component.key
    )::DOUBLE PRECISION AS marginal_volatility_contribution,
    component.value::DOUBLE PRECISION AS component_volatility_contribution,
    (
        attribution.component_contribution_share_json
        ->> component.key
    )::DOUBLE PRECISION AS contribution_share,
    attribution.portfolio_volatility_annualized,
    attribution.volatility_status
FROM risk_platform.latest_portfolio_risk_attribution attribution
CROSS JOIN LATERAL jsonb_each_text(
    attribution.component_volatility_contribution_json
) component;
