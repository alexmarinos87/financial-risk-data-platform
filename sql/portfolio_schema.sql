-- PostgreSQL serving contract for versioned portfolio daily risk outputs.
-- Apply after sql/postgres_schema.sql so the daily-return source tables already exist.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.portfolio_daily_returns (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    base_currency CHAR(3) NOT NULL,
    definition_fingerprint TEXT NOT NULL,
    weighting_method TEXT NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    constituent_count INTEGER NOT NULL CHECK (
        constituent_count >= 2 AND constituent_count <= 50
    ),
    weights_json JSONB NOT NULL,
    component_calculation_ids_json JSONB NOT NULL,
    component_returns_json JSONB NOT NULL,
    contributions_json JSONB NOT NULL,
    portfolio_return_1d DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (portfolio_id <> ''),
    CHECK (base_currency = UPPER(base_currency)),
    CHECK (definition_fingerprint <> ''),
    CHECK (weighting_method <> ''),
    CHECK (
        model_version <> 'portfolio-risk-v1'
        OR weighting_method = 'constant_weight_daily_rebalanced'
    ),
    CHECK (ts_ingest >= ts_event),
    CHECK (jsonb_typeof(weights_json) = 'object'),
    CHECK (jsonb_typeof(component_calculation_ids_json) = 'object'),
    CHECK (jsonb_typeof(component_returns_json) = 'object'),
    CHECK (jsonb_typeof(contributions_json) = 'object'),
    CHECK (
        portfolio_return_1d > -1
        AND portfolio_return_1d < 'Infinity'::DOUBLE PRECISION
    )
);

CREATE INDEX IF NOT EXISTS idx_portfolio_daily_returns_version_lookup
    ON risk_platform.portfolio_daily_returns (
        portfolio_id,
        definition_fingerprint,
        ts_event DESC,
        model_version,
        weighting_method,
        ts_ingest DESC,
        calculation_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_portfolio_daily_returns_components
    ON risk_platform.portfolio_daily_returns
    USING GIN (component_calculation_ids_json);

CREATE TABLE IF NOT EXISTS risk_platform.portfolio_daily_risk_summary (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    base_currency CHAR(3) NOT NULL,
    definition_fingerprint TEXT NOT NULL,
    weighting_method TEXT NOT NULL,
    portfolio_return_calculation_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_daily_returns (calculation_id),
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    portfolio_return_1d DOUBLE PRECISION NOT NULL,
    volatility_annualized DOUBLE PRECISION,
    volatility_window INTEGER NOT NULL CHECK (volatility_window >= 2),
    annualization_days INTEGER NOT NULL CHECK (annualization_days > 0),
    historical_var_loss DOUBLE PRECISION,
    var_confidence DOUBLE PRECISION NOT NULL CHECK (
        var_confidence > 0 AND var_confidence < 1
    ),
    var_window INTEGER NOT NULL CHECK (var_window >= 2),
    var_observations INTEGER NOT NULL CHECK (var_observations >= 0),
    maximum_drawdown DOUBLE PRECISION NOT NULL CHECK (
        maximum_drawdown >= -1 AND maximum_drawdown <= 0
    ),
    aligned_observations INTEGER NOT NULL CHECK (aligned_observations >= 1),
    constituent_count INTEGER NOT NULL CHECK (
        constituent_count >= 2 AND constituent_count <= 50
    ),
    weights_json JSONB NOT NULL,
    history_status TEXT NOT NULL CHECK (history_status IN ('partial', 'ready')),
    input_first_calculation_id TEXT NOT NULL,
    input_last_calculation_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (portfolio_id <> ''),
    CHECK (base_currency = UPPER(base_currency)),
    CHECK (definition_fingerprint <> ''),
    CHECK (weighting_method <> ''),
    CHECK (portfolio_return_calculation_id <> ''),
    CHECK (input_first_calculation_id <> ''),
    CHECK (input_last_calculation_id <> ''),
    CHECK (
        model_version <> 'portfolio-risk-v1'
        OR weighting_method = 'constant_weight_daily_rebalanced'
    ),
    CHECK (ts_ingest >= ts_event),
    CHECK (jsonb_typeof(weights_json) = 'object'),
    CHECK (
        portfolio_return_1d > -1
        AND portfolio_return_1d < 'Infinity'::DOUBLE PRECISION
    ),
    CHECK (
        volatility_annualized IS NULL
        OR (
            volatility_annualized >= 0
            AND volatility_annualized < 'Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        historical_var_loss IS NULL
        OR (
            historical_var_loss >= 0
            AND historical_var_loss < 'Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (var_observations <= var_window),
    CHECK (var_observations <= aligned_observations),
    CHECK (
        history_status <> 'ready'
        OR (
            aligned_observations >= GREATEST(volatility_window, var_window)
            AND var_observations >= var_window
            AND volatility_annualized IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_portfolio_daily_risk_version_lookup
    ON risk_platform.portfolio_daily_risk_summary (
        portfolio_id,
        definition_fingerprint,
        ts_event DESC,
        model_version,
        weighting_method,
        volatility_window,
        var_window,
        var_confidence,
        annualization_days,
        ts_ingest DESC,
        calculation_id DESC
    );

CREATE OR REPLACE VIEW risk_platform.latest_portfolio_daily_returns AS
SELECT
    calculation_id,
    model_version,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    weighting_method,
    ts_event,
    ts_ingest,
    constituent_count,
    weights_json,
    component_calculation_ids_json,
    component_returns_json,
    contributions_json,
    portfolio_return_1d
FROM (
    SELECT
        portfolio_return.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                portfolio_id,
                definition_fingerprint,
                ts_event,
                model_version,
                weighting_method
            ORDER BY ts_ingest DESC, calculation_id DESC
        ) AS version_rank
    FROM risk_platform.portfolio_daily_returns portfolio_return
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.latest_portfolio_daily_risk_summary AS
SELECT
    calculation_id,
    model_version,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    weighting_method,
    portfolio_return_calculation_id,
    ts_event,
    ts_ingest,
    portfolio_return_1d,
    volatility_annualized,
    volatility_window,
    annualization_days,
    historical_var_loss,
    var_confidence,
    var_window,
    var_observations,
    maximum_drawdown,
    aligned_observations,
    constituent_count,
    weights_json,
    history_status,
    input_first_calculation_id,
    input_last_calculation_id
FROM (
    SELECT
        summary.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                portfolio_id,
                definition_fingerprint,
                ts_event,
                model_version,
                weighting_method,
                volatility_window,
                var_window,
                var_confidence,
                annualization_days
            ORDER BY ts_ingest DESC, calculation_id DESC
        ) AS version_rank
    FROM risk_platform.portfolio_daily_risk_summary summary
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_semantic_model AS
SELECT
    calculation_id,
    model_version,
    portfolio_id,
    base_currency,
    definition_fingerprint,
    weighting_method,
    ts_event AS metric_ts,
    ts_ingest AS calculation_ts,
    portfolio_return_1d,
    volatility_annualized,
    volatility_window,
    annualization_days,
    historical_var_loss,
    var_confidence,
    var_window,
    var_observations,
    maximum_drawdown,
    aligned_observations,
    constituent_count,
    weights_json,
    history_status
FROM risk_platform.latest_portfolio_daily_risk_summary;

CREATE OR REPLACE VIEW risk_platform.portfolio_daily_contribution_model AS
SELECT
    portfolio_return.calculation_id AS portfolio_return_calculation_id,
    portfolio_return.model_version,
    portfolio_return.portfolio_id,
    portfolio_return.base_currency,
    portfolio_return.definition_fingerprint,
    portfolio_return.weighting_method,
    portfolio_return.ts_event AS metric_ts,
    portfolio_return.ts_ingest AS calculation_ts,
    contribution.key AS constituent_key,
    (portfolio_return.weights_json ->> contribution.key)::DOUBLE PRECISION AS weight,
    (portfolio_return.component_returns_json ->> contribution.key)::DOUBLE PRECISION
        AS component_return_1d,
    portfolio_return.component_calculation_ids_json ->> contribution.key
        AS component_calculation_id,
    contribution.value::DOUBLE PRECISION AS contribution_1d
FROM risk_platform.latest_portfolio_daily_returns portfolio_return
CROSS JOIN LATERAL jsonb_each_text(
    portfolio_return.contributions_json
) contribution;
