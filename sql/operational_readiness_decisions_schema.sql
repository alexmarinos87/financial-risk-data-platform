-- Append-only operational readiness decision history and current serving views.
-- Apply after sql/operational_service_levels_schema.sql so retained report evidence exists.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.operational_readiness_decisions (
    decision_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'operational-readiness-gate-v1'
    ),
    gate_id TEXT NOT NULL,
    gate_fingerprint TEXT NOT NULL,
    operational_policy_id TEXT NOT NULL,
    operational_policy_fingerprint TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    schedule_fingerprint TEXT NOT NULL,
    calendar_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    risk_limit_policy_id TEXT NOT NULL,
    mandate_fingerprint TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    latest_expected_session DATE NOT NULL,
    max_report_age_seconds INTEGER NOT NULL CHECK (
        max_report_age_seconds BETWEEN 1 AND 604800
    ),
    allow_warning BOOLEAN NOT NULL,
    report_calculation_id TEXT REFERENCES
        risk_platform.operational_service_level_reports (calculation_id),
    report_document_sha256 TEXT,
    report_as_of TIMESTAMPTZ,
    report_latest_expected_session DATE,
    report_status TEXT CHECK (
        report_status IS NULL OR report_status IN ('ok', 'warning', 'critical')
    ),
    report_age_seconds DOUBLE PRECISION CHECK (
        report_age_seconds IS NULL
        OR (
            report_age_seconds >= 0
            AND report_age_seconds < 'Infinity'::DOUBLE PRECISION
        )
    ),
    report_future_seconds DOUBLE PRECISION CHECK (
        report_future_seconds IS NULL
        OR (
            report_future_seconds >= 0
            AND report_future_seconds < 'Infinity'::DOUBLE PRECISION
        )
    ),
    decision TEXT NOT NULL CHECK (decision IN ('allow', 'block')),
    reasons JSONB NOT NULL,
    schedule_executed BOOLEAN NOT NULL,
    provider_request_performed BOOLEAN NOT NULL,
    notification_delivery_performed BOOLEAN NOT NULL,
    cloud_schedule_activated BOOLEAN NOT NULL,
    decision_json JSONB NOT NULL,
    document_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        decision_id
            ~ '^operational-readiness-gate-v1-decision-[0-9a-f]{24}$'
    ),
    CHECK (gate_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        gate_fingerprint ~ '^operational-readiness-gate-[0-9a-f]{24}$'
    ),
    CHECK (
        operational_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (
        operational_policy_fingerprint
            ~ '^operational-slo-policy-[0-9a-f]{24}$'
    ),
    CHECK (schedule_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (schedule_fingerprint <> ''),
    CHECK (calendar_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (portfolio_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        risk_limit_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (mandate_fingerprint <> ''),
    CHECK (jsonb_typeof(reasons) = 'array'),
    CHECK (
        (decision = 'allow' AND jsonb_array_length(reasons) = 0)
        OR (decision = 'block' AND jsonb_array_length(reasons) > 0)
    ),
    CHECK (
        (
            report_calculation_id IS NULL
            AND report_document_sha256 IS NULL
            AND report_as_of IS NULL
            AND report_latest_expected_session IS NULL
            AND report_status IS NULL
            AND report_age_seconds IS NULL
            AND report_future_seconds IS NULL
            AND reasons = '["report_missing"]'::JSONB
        )
        OR (
            report_calculation_id IS NOT NULL
            AND report_document_sha256 IS NOT NULL
            AND report_as_of IS NOT NULL
            AND report_latest_expected_session IS NOT NULL
            AND report_status IS NOT NULL
            AND report_age_seconds IS NOT NULL
            AND report_future_seconds IS NOT NULL
        )
    ),
    CHECK (
        report_document_sha256 IS NULL
        OR report_document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (NOT schedule_executed),
    CHECK (NOT provider_request_performed),
    CHECK (NOT notification_delivery_performed),
    CHECK (NOT cloud_schedule_activated),
    CHECK (jsonb_typeof(decision_json) = 'object'),
    CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (decision_json ->> 'decision_id' = decision_id),
    CHECK (decision_json ->> 'model_version' = model_version),
    CHECK (decision_json ->> 'gate_id' = gate_id),
    CHECK (decision_json ->> 'gate_fingerprint' = gate_fingerprint),
    CHECK (
        decision_json ->> 'operational_policy_id' = operational_policy_id
    ),
    CHECK (
        decision_json ->> 'operational_policy_fingerprint'
            = operational_policy_fingerprint
    ),
    CHECK (decision_json ->> 'schedule_id' = schedule_id),
    CHECK (
        decision_json ->> 'schedule_fingerprint' = schedule_fingerprint
    ),
    CHECK (decision_json ->> 'calendar_id' = calendar_id),
    CHECK (decision_json ->> 'portfolio_id' = portfolio_id),
    CHECK (
        decision_json ->> 'risk_limit_policy_id' = risk_limit_policy_id
    ),
    CHECK (
        decision_json ->> 'mandate_fingerprint' = mandate_fingerprint
    ),
    CHECK (
        (decision_json ->> 'evaluated_at')::TIMESTAMPTZ = evaluated_at
    ),
    CHECK (
        (decision_json ->> 'latest_expected_session')::DATE
            = latest_expected_session
    ),
    CHECK (
        (decision_json ->> 'max_report_age_seconds')::INTEGER
            = max_report_age_seconds
    ),
    CHECK ((decision_json ->> 'allow_warning')::BOOLEAN = allow_warning),
    CHECK (
        decision_json ->> 'report_calculation_id'
            IS NOT DISTINCT FROM report_calculation_id
    ),
    CHECK (
        decision_json ->> 'report_document_sha256'
            IS NOT DISTINCT FROM report_document_sha256
    ),
    CHECK (
        (decision_json ->> 'report_as_of')::TIMESTAMPTZ
            IS NOT DISTINCT FROM report_as_of
    ),
    CHECK (
        (decision_json ->> 'report_latest_expected_session')::DATE
            IS NOT DISTINCT FROM report_latest_expected_session
    ),
    CHECK (
        decision_json ->> 'report_status' IS NOT DISTINCT FROM report_status
    ),
    CHECK (
        (decision_json ->> 'report_age_seconds')::DOUBLE PRECISION
            IS NOT DISTINCT FROM report_age_seconds
    ),
    CHECK (
        (decision_json ->> 'report_future_seconds')::DOUBLE PRECISION
            IS NOT DISTINCT FROM report_future_seconds
    ),
    CHECK (decision_json ->> 'decision' = decision),
    CHECK (decision_json -> 'reasons' = reasons)
);

CREATE INDEX IF NOT EXISTS idx_operational_readiness_decisions_latest
    ON risk_platform.operational_readiness_decisions (
        model_version,
        gate_id,
        gate_fingerprint,
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session DESC,
        evaluated_at DESC,
        decision_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_operational_readiness_report_reference
    ON risk_platform.operational_readiness_decisions (report_calculation_id)
    WHERE report_calculation_id IS NOT NULL;

CREATE OR REPLACE FUNCTION
    risk_platform.prevent_operational_readiness_decision_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'operational readiness decisions are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS prevent_operational_readiness_decision_update
    ON risk_platform.operational_readiness_decisions;
CREATE TRIGGER prevent_operational_readiness_decision_update
BEFORE UPDATE ON risk_platform.operational_readiness_decisions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_operational_readiness_decision_mutation();

DROP TRIGGER IF EXISTS prevent_operational_readiness_decision_delete
    ON risk_platform.operational_readiness_decisions;
CREATE TRIGGER prevent_operational_readiness_decision_delete
BEFORE DELETE ON risk_platform.operational_readiness_decisions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_operational_readiness_decision_mutation();

CREATE OR REPLACE VIEW risk_platform.operational_readiness_decision_history AS
SELECT
    decision_id,
    model_version,
    gate_id,
    gate_fingerprint,
    operational_policy_id,
    operational_policy_fingerprint,
    schedule_id,
    schedule_fingerprint,
    calendar_id,
    portfolio_id,
    risk_limit_policy_id,
    mandate_fingerprint,
    evaluated_at,
    latest_expected_session,
    max_report_age_seconds,
    allow_warning,
    report_calculation_id,
    report_document_sha256,
    report_as_of,
    report_latest_expected_session,
    report_status,
    report_age_seconds,
    report_future_seconds,
    decision,
    reasons,
    jsonb_array_length(reasons) AS reason_count,
    document_sha256,
    recorded_at
FROM risk_platform.operational_readiness_decisions;

CREATE OR REPLACE VIEW risk_platform.operational_readiness_reason_history AS
SELECT
    decision.decision_id,
    decision.model_version,
    decision.gate_id,
    decision.gate_fingerprint,
    decision.schedule_id,
    decision.schedule_fingerprint,
    decision.portfolio_id,
    decision.latest_expected_session,
    decision.evaluated_at,
    reason.ordinality::INTEGER AS reason_ordinal,
    reason.value AS reason
FROM risk_platform.operational_readiness_decisions decision
CROSS JOIN LATERAL jsonb_array_elements_text(decision.reasons)
    WITH ORDINALITY AS reason(value, ordinality);

CREATE OR REPLACE VIEW risk_platform.latest_operational_readiness_decisions AS
SELECT
    decision_id,
    model_version,
    gate_id,
    gate_fingerprint,
    operational_policy_id,
    operational_policy_fingerprint,
    schedule_id,
    schedule_fingerprint,
    calendar_id,
    portfolio_id,
    risk_limit_policy_id,
    mandate_fingerprint,
    evaluated_at,
    latest_expected_session,
    max_report_age_seconds,
    allow_warning,
    report_calculation_id,
    report_document_sha256,
    report_as_of,
    report_latest_expected_session,
    report_status,
    report_age_seconds,
    report_future_seconds,
    decision,
    reasons,
    document_sha256,
    recorded_at
FROM (
    SELECT
        readiness.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                model_version,
                gate_id,
                gate_fingerprint,
                operational_policy_id,
                operational_policy_fingerprint,
                schedule_id,
                schedule_fingerprint,
                calendar_id,
                portfolio_id,
                risk_limit_policy_id,
                mandate_fingerprint,
                latest_expected_session
            ORDER BY evaluated_at DESC, decision_id DESC
        ) AS decision_rank
    FROM risk_platform.operational_readiness_decisions readiness
) ranked
WHERE decision_rank = 1;

CREATE OR REPLACE VIEW
    risk_platform.current_allowed_operational_readiness_decisions AS
SELECT *
FROM risk_platform.latest_operational_readiness_decisions
WHERE decision = 'allow';

CREATE OR REPLACE VIEW
    risk_platform.current_blocked_operational_readiness_decisions AS
SELECT *
FROM risk_platform.latest_operational_readiness_decisions
WHERE decision = 'block';
