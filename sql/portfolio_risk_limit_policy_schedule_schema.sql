-- Effective-dated policy metadata for portfolio risk-limit evaluations.
-- Apply after sql/portfolio_risk_limits_schema.sql.

ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    ADD COLUMN IF NOT EXISTS policy_effective_from DATE;
ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    ADD COLUMN IF NOT EXISTS policy_effective_to DATE;
ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    ADD COLUMN IF NOT EXISTS policy_period_source TEXT;

-- Legacy local rows were produced before policy periods were explicit. Their
-- original configuration does not prove a wider validity period, so migrate
-- each event as a one-day inferred period rather than inventing a mandate.
UPDATE risk_platform.portfolio_risk_limit_evaluations
SET
    policy_effective_from = (ts_event AT TIME ZONE 'UTC')::DATE,
    policy_effective_to = (ts_event AT TIME ZONE 'UTC')::DATE + 1,
    policy_period_source = 'inferred_event_date'
WHERE
    policy_effective_from IS NULL
    OR policy_period_source IS NULL;

ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    ALTER COLUMN policy_effective_from SET NOT NULL;
ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    ALTER COLUMN policy_period_source SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'portfolio_risk_limit_policy_period_source'
          AND conrelid = 'risk_platform.portfolio_risk_limit_evaluations'::REGCLASS
    ) THEN
        ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
            ADD CONSTRAINT portfolio_risk_limit_policy_period_source
            CHECK (
                policy_period_source IN (
                    'configured',
                    'legacy_unbounded',
                    'inferred_event_date'
                )
            ) NOT VALID;
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'portfolio_risk_limit_policy_period_order'
          AND conrelid = 'risk_platform.portfolio_risk_limit_evaluations'::REGCLASS
    ) THEN
        ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
            ADD CONSTRAINT portfolio_risk_limit_policy_period_order
            CHECK (
                policy_effective_to IS NULL
                OR policy_effective_to > policy_effective_from
            ) NOT VALID;
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'portfolio_risk_limit_policy_period_covers_event'
          AND conrelid = 'risk_platform.portfolio_risk_limit_evaluations'::REGCLASS
    ) THEN
        ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
            ADD CONSTRAINT portfolio_risk_limit_policy_period_covers_event
            CHECK (
                ts_event >= (policy_effective_from::TIMESTAMP AT TIME ZONE 'UTC')
                AND (
                    policy_effective_to IS NULL
                    OR ts_event < (policy_effective_to::TIMESTAMP AT TIME ZONE 'UTC')
                )
            ) NOT VALID;
    END IF;
END;
$$;

ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    VALIDATE CONSTRAINT portfolio_risk_limit_policy_period_source;
ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    VALIDATE CONSTRAINT portfolio_risk_limit_policy_period_order;
ALTER TABLE risk_platform.portfolio_risk_limit_evaluations
    VALIDATE CONSTRAINT portfolio_risk_limit_policy_period_covers_event;

CREATE INDEX IF NOT EXISTS idx_portfolio_risk_limit_policy_period
    ON risk_platform.portfolio_risk_limit_evaluations (
        policy_id,
        portfolio_id,
        policy_effective_from,
        policy_effective_to,
        policy_period_source,
        policy_fingerprint,
        ts_event DESC
    );

CREATE OR REPLACE VIEW risk_platform.latest_portfolio_risk_limit_policy_evaluations AS
SELECT
    latest.*,
    stored.policy_effective_from,
    stored.policy_effective_to,
    stored.policy_period_source
FROM risk_platform.latest_portfolio_risk_limit_evaluations latest
JOIN risk_platform.portfolio_risk_limit_evaluations stored
    ON stored.calculation_id = latest.calculation_id;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_policy_breaches AS
SELECT *
FROM risk_platform.latest_portfolio_risk_limit_policy_evaluations
WHERE is_breach;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_policy_snapshot_status AS
SELECT
    policy_id,
    policy_fingerprint,
    policy_effective_from,
    policy_effective_to,
    policy_period_source,
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
FROM risk_platform.latest_portfolio_risk_limit_policy_evaluations
GROUP BY
    policy_id,
    policy_fingerprint,
    policy_effective_from,
    policy_effective_to,
    policy_period_source,
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

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_policy_versions_observed AS
SELECT
    policy_id,
    policy_fingerprint,
    portfolio_id,
    policy_effective_from,
    policy_effective_to,
    policy_period_source,
    covariance_window,
    annualization_days,
    (MIN(ts_event) AT TIME ZONE 'UTC')::DATE AS first_observed_event_date,
    (MAX(ts_event) AT TIME ZONE 'UTC')::DATE AS last_observed_event_date,
    COUNT(DISTINCT (ts_event AT TIME ZONE 'UTC')::DATE) AS observed_event_dates,
    MAX(warning_threshold) FILTER (
        WHERE metric_name = 'portfolio_volatility_annualized'
    ) AS portfolio_volatility_warning,
    MAX(critical_threshold) FILTER (
        WHERE metric_name = 'portfolio_volatility_annualized'
    ) AS portfolio_volatility_critical,
    MAX(warning_threshold) FILTER (
        WHERE metric_name = 'largest_absolute_component_contribution_share'
    ) AS component_concentration_warning,
    MAX(critical_threshold) FILTER (
        WHERE metric_name = 'largest_absolute_component_contribution_share'
    ) AS component_concentration_critical
FROM risk_platform.portfolio_risk_limit_evaluations
GROUP BY
    policy_id,
    policy_fingerprint,
    portfolio_id,
    policy_effective_from,
    policy_effective_to,
    policy_period_source,
    covariance_window,
    annualization_days;
