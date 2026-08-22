-- Durable notification candidates for portfolio risk-limit lifecycle transitions.
-- Apply after sql/portfolio_risk_breach_lifecycle_schema.sql.
-- This schema does not deliver messages to external systems.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.portfolio_risk_notification_outbox (
    event_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'breach_opened',
            'breach_escalated',
            'breach_deescalated',
            'breach_resolved'
        )
    ),
    transition_type TEXT NOT NULL CHECK (
        transition_type IN ('opened', 'escalated', 'deescalated', 'resolved')
    ),
    delivery_disposition TEXT NOT NULL CHECK (
        delivery_disposition IN ('pending', 'suppressed')
    ),
    suppression_reason TEXT,
    source_evaluation_calculation_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_risk_limit_evaluations (calculation_id),
    source_previous_evaluation_calculation_id TEXT REFERENCES
        risk_platform.portfolio_risk_limit_evaluations (calculation_id),
    risk_limit_model_version TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    base_currency CHAR(3) NOT NULL,
    definition_fingerprint TEXT NOT NULL,
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
    previous_subject_key TEXT,
    subject_changed BOOLEAN NOT NULL,
    unit TEXT NOT NULL CHECK (
        unit IN ('annualized_decimal', 'absolute_share')
    ),
    previous_status TEXT CHECK (
        previous_status IN ('ok', 'warning', 'critical')
    ),
    current_status TEXT NOT NULL CHECK (
        current_status IN ('ok', 'warning', 'critical')
    ),
    severity_rank INTEGER NOT NULL CHECK (severity_rank IN (0, 1, 2)),
    observed_value DOUBLE PRECISION NOT NULL,
    observed_signed_value DOUBLE PRECISION NOT NULL,
    warning_threshold DOUBLE PRECISION NOT NULL,
    critical_threshold DOUBLE PRECISION NOT NULL,
    breach_excess DOUBLE PRECISION NOT NULL,
    payload_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (event_id <> ''),
    CHECK (model_version <> ''),
    CHECK (source_evaluation_calculation_id <> ''),
    CHECK (
        source_previous_evaluation_calculation_id IS NULL
        OR source_previous_evaluation_calculation_id <> ''
    ),
    CHECK (risk_limit_model_version <> ''),
    CHECK (policy_id <> ''),
    CHECK (policy_fingerprint <> ''),
    CHECK (portfolio_id <> ''),
    CHECK (base_currency = UPPER(base_currency)),
    CHECK (definition_fingerprint <> ''),
    CHECK (attribution_model_version <> ''),
    CHECK (weighting_method <> ''),
    CHECK (covariance_method <> ''),
    CHECK (correlation_method <> ''),
    CHECK (subject_key <> ''),
    CHECK (previous_subject_key IS NULL OR previous_subject_key <> ''),
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
        (current_status = 'ok' AND severity_rank = 0)
        OR (current_status = 'warning' AND severity_rank = 1)
        OR (current_status = 'critical' AND severity_rank = 2)
    ),
    CHECK (
        subject_changed = (
            previous_subject_key IS NOT NULL
            AND previous_subject_key <> subject_key
        )
    ),
    CHECK (
        (transition_type = 'opened'
            AND (previous_status IS NULL OR previous_status = 'ok')
            AND current_status IN ('warning', 'critical'))
        OR (transition_type = 'escalated'
            AND previous_status = 'warning'
            AND current_status = 'critical')
        OR (transition_type = 'deescalated'
            AND previous_status = 'critical'
            AND current_status = 'warning')
        OR (transition_type = 'resolved'
            AND previous_status IN ('warning', 'critical')
            AND current_status = 'ok')
    ),
    CHECK (
        (previous_status IS NULL
            AND source_previous_evaluation_calculation_id IS NULL)
        OR (previous_status IS NOT NULL
            AND source_previous_evaluation_calculation_id IS NOT NULL)
    ),
    CHECK (
        (transition_type = 'opened' AND event_type = 'breach_opened')
        OR (transition_type = 'escalated'
            AND event_type = 'breach_escalated')
        OR (transition_type = 'deescalated'
            AND event_type = 'breach_deescalated')
        OR (transition_type = 'resolved'
            AND event_type = 'breach_resolved')
    ),
    CHECK (
        (transition_type IN ('opened', 'escalated', 'resolved')
            AND delivery_disposition = 'pending'
            AND suppression_reason IS NULL)
        OR (transition_type = 'deescalated'
            AND delivery_disposition = 'suppressed'
            AND suppression_reason = 'deescalation_not_routed')
    ),
    CHECK (
        (metric_name = 'portfolio_volatility_annualized'
            AND subject_type = 'portfolio'
            AND subject_key = portfolio_id
            AND unit = 'annualized_decimal'
            AND observed_signed_value = observed_value)
        OR (metric_name
                = 'largest_absolute_component_contribution_share'
            AND subject_type = 'constituent'
            AND unit = 'absolute_share'
            AND ABS(ABS(observed_signed_value) - observed_value)
                <= 0.0000000001)
    ),
    CHECK (
        model_version <> 'portfolio-risk-notification-outbox-v1'
        OR (
            risk_limit_model_version = 'portfolio-risk-limits-v1'
            AND attribution_model_version = 'portfolio-attribution-v1'
            AND weighting_method = 'constant_weight_daily_rebalanced'
            AND covariance_method = 'sample_annualized'
            AND correlation_method = 'pearson'
        )
    ),
    CHECK (jsonb_typeof(payload_json) = 'object'),
    CHECK (payload_json ->> 'event_id' = event_id),
    CHECK (payload_json ->> 'event_type' = event_type),
    CHECK (payload_json ->> 'transition_type' = transition_type),
    CHECK (
        payload_json #>> '{policy,policy_id}' = policy_id
        AND payload_json #>> '{policy,policy_fingerprint}'
            = policy_fingerprint
    ),
    CHECK (
        payload_json #>> '{portfolio,portfolio_id}' = portfolio_id
        AND payload_json #>> '{portfolio,definition_fingerprint}'
            = definition_fingerprint
        AND payload_json #>> '{portfolio,base_currency}' = base_currency
    ),
    CHECK (
        payload_json #>> '{source,evaluation_calculation_id}'
            = source_evaluation_calculation_id
    )
);

CREATE INDEX IF NOT EXISTS idx_portfolio_notification_outbox_lookup
    ON risk_platform.portfolio_risk_notification_outbox (
        policy_id,
        policy_fingerprint,
        portfolio_id,
        definition_fingerprint,
        ts_event DESC,
        metric_name,
        transition_type,
        loaded_at DESC,
        event_id
    );

CREATE INDEX IF NOT EXISTS idx_portfolio_notification_outbox_pending
    ON risk_platform.portfolio_risk_notification_outbox (
        policy_id,
        portfolio_id,
        ts_event,
        event_type
    )
    WHERE delivery_disposition = 'pending';

CREATE OR REPLACE VIEW
risk_platform.current_portfolio_risk_notification_outbox AS
SELECT
    outbox.event_id,
    outbox.model_version,
    outbox.event_type,
    outbox.transition_type,
    outbox.delivery_disposition,
    outbox.suppression_reason,
    outbox.source_evaluation_calculation_id,
    outbox.source_previous_evaluation_calculation_id,
    outbox.risk_limit_model_version,
    outbox.policy_id,
    outbox.policy_fingerprint,
    outbox.portfolio_id,
    outbox.base_currency,
    outbox.definition_fingerprint,
    outbox.attribution_model_version,
    outbox.weighting_method,
    outbox.covariance_method,
    outbox.correlation_method,
    outbox.covariance_window,
    outbox.annualization_days,
    outbox.ts_event,
    outbox.ts_ingest,
    outbox.metric_name,
    outbox.subject_type,
    outbox.subject_key,
    outbox.previous_subject_key,
    outbox.subject_changed,
    outbox.unit,
    outbox.previous_status,
    outbox.current_status,
    outbox.severity_rank,
    outbox.observed_value,
    outbox.observed_signed_value,
    outbox.warning_threshold,
    outbox.critical_threshold,
    outbox.breach_excess,
    outbox.payload_json,
    outbox.loaded_at
FROM risk_platform.portfolio_risk_notification_outbox outbox
JOIN risk_platform.portfolio_risk_limit_actionable_transitions transition
  ON transition.calculation_id
        = outbox.source_evaluation_calculation_id
 AND transition.transition_type = outbox.transition_type
 AND (
        transition.previous_calculation_id
            = outbox.source_previous_evaluation_calculation_id
        OR (
            transition.previous_calculation_id IS NULL
            AND outbox.source_previous_evaluation_calculation_id IS NULL
        )
    );

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_notification_pending AS
SELECT *
FROM risk_platform.current_portfolio_risk_notification_outbox
WHERE delivery_disposition = 'pending';

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_notification_suppressed AS
SELECT *
FROM risk_platform.current_portfolio_risk_notification_outbox
WHERE delivery_disposition = 'suppressed';

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_notification_outbox_summary AS
SELECT
    policy_id,
    policy_fingerprint,
    portfolio_id,
    definition_fingerprint,
    metric_name,
    event_type,
    transition_type,
    delivery_disposition,
    COUNT(*) AS event_count,
    MIN(ts_event) AS first_event_time,
    MAX(ts_event) AS last_event_time
FROM risk_platform.current_portfolio_risk_notification_outbox
GROUP BY
    policy_id,
    policy_fingerprint,
    portfolio_id,
    definition_fingerprint,
    metric_name,
    event_type,
    transition_type,
    delivery_disposition;
