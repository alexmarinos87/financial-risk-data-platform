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

-- Human acknowledgements are immutable evidence attached to current breaches.
-- They do not modify or delete the underlying risk-limit evaluation.
CREATE TABLE IF NOT EXISTS risk_platform.portfolio_risk_limit_acknowledgements (
    acknowledgement_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'portfolio-risk-limit-ack-v1'
    ),
    evaluation_calculation_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_risk_limit_evaluations (calculation_id),
    request_id TEXT NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL,
    acknowledged_by TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('investigating', 'accepted', 'false_positive')
    ),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (evaluation_calculation_id, request_id),
    CHECK (acknowledgement_id <> ''),
    CHECK (request_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        acknowledged_by = BTRIM(acknowledged_by)
        AND CHAR_LENGTH(acknowledged_by) BETWEEN 1 AND 128
        AND acknowledged_by !~ '[[:cntrl:]]'
    ),
    CHECK (
        reason = BTRIM(reason)
        AND CHAR_LENGTH(reason) BETWEEN 1 AND 2000
        AND reason !~ '[[:cntrl:]]'
    )
);

CREATE INDEX IF NOT EXISTS idx_portfolio_risk_limit_acknowledgements_target
    ON risk_platform.portfolio_risk_limit_acknowledgements (
        evaluation_calculation_id,
        acknowledged_at DESC,
        acknowledgement_id DESC
    );

CREATE OR REPLACE FUNCTION risk_platform.validate_risk_limit_acknowledgement()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_is_breach BOOLEAN;
    target_ts_event TIMESTAMPTZ;
BEGIN
    SELECT evaluation.is_breach, evaluation.ts_event
    INTO target_is_breach, target_ts_event
    FROM risk_platform.portfolio_risk_limit_evaluations evaluation
    WHERE evaluation.calculation_id = NEW.evaluation_calculation_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'risk-limit acknowledgement target does not exist';
    END IF;
    IF NOT target_is_breach THEN
        RAISE EXCEPTION 'risk-limit acknowledgement target must be a breach';
    END IF;
    IF NEW.acknowledged_at < target_ts_event THEN
        RAISE EXCEPTION 'acknowledged_at must be on or after the breach event';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION risk_platform.prevent_risk_limit_acknowledgement_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'risk-limit acknowledgements are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS validate_risk_limit_acknowledgement_insert
    ON risk_platform.portfolio_risk_limit_acknowledgements;
CREATE TRIGGER validate_risk_limit_acknowledgement_insert
BEFORE INSERT ON risk_platform.portfolio_risk_limit_acknowledgements
FOR EACH ROW
EXECUTE FUNCTION risk_platform.validate_risk_limit_acknowledgement();

DROP TRIGGER IF EXISTS prevent_risk_limit_acknowledgement_update
    ON risk_platform.portfolio_risk_limit_acknowledgements;
CREATE TRIGGER prevent_risk_limit_acknowledgement_update
BEFORE UPDATE ON risk_platform.portfolio_risk_limit_acknowledgements
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_risk_limit_acknowledgement_mutation();

DROP TRIGGER IF EXISTS prevent_risk_limit_acknowledgement_delete
    ON risk_platform.portfolio_risk_limit_acknowledgements;
CREATE TRIGGER prevent_risk_limit_acknowledgement_delete
BEFORE DELETE ON risk_platform.portfolio_risk_limit_acknowledgements
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_risk_limit_acknowledgement_mutation();

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_acknowledgement_history AS
SELECT
    acknowledgement.acknowledgement_id,
    acknowledgement.model_version,
    acknowledgement.evaluation_calculation_id,
    acknowledgement.request_id,
    acknowledgement.acknowledged_at,
    acknowledgement.acknowledged_by,
    acknowledgement.disposition,
    acknowledgement.reason,
    acknowledgement.created_at,
    evaluation.policy_id,
    evaluation.policy_fingerprint,
    evaluation.portfolio_id,
    evaluation.definition_fingerprint,
    evaluation.ts_event AS metric_ts,
    evaluation.metric_name,
    evaluation.subject_type,
    evaluation.subject_key,
    evaluation.status AS breach_status,
    evaluation.observed_value,
    evaluation.breach_threshold,
    evaluation.breach_excess
FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
JOIN risk_platform.portfolio_risk_limit_evaluations evaluation
    ON evaluation.calculation_id = acknowledgement.evaluation_calculation_id;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_breach_status AS
SELECT
    breach.*,
    COALESCE(latest_acknowledgement.acknowledgement_count, 0)
        AS acknowledgement_count,
    latest_acknowledgement.acknowledgement_id,
    latest_acknowledgement.request_id AS latest_acknowledgement_request_id,
    latest_acknowledgement.acknowledged_at AS latest_acknowledged_at,
    latest_acknowledgement.acknowledged_by AS latest_acknowledged_by,
    latest_acknowledgement.disposition AS latest_acknowledgement_disposition,
    latest_acknowledgement.reason AS latest_acknowledgement_reason,
    CASE
        WHEN latest_acknowledgement.acknowledgement_id IS NULL
            THEN 'unacknowledged'
        ELSE 'acknowledged'
    END AS acknowledgement_status
FROM risk_platform.portfolio_risk_limit_breaches breach
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) OVER () AS acknowledgement_count,
        acknowledgement.acknowledgement_id,
        acknowledgement.request_id,
        acknowledgement.acknowledged_at,
        acknowledgement.acknowledged_by,
        acknowledgement.disposition,
        acknowledgement.reason
    FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
    WHERE acknowledgement.evaluation_calculation_id = breach.calculation_id
    ORDER BY
        acknowledgement.acknowledged_at DESC,
        acknowledgement.acknowledgement_id DESC
    LIMIT 1
) latest_acknowledgement ON TRUE;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_open_breaches AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_status
WHERE acknowledgement_status = 'unacknowledged';

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_acknowledged_breaches AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_status
WHERE acknowledgement_status = 'acknowledged';
