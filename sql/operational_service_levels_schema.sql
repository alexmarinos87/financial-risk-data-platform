-- Append-only operational service-level report history and serving views.
-- Apply after the market-freshness and notification-delivery schemas.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.operational_service_level_reports (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'operational-service-levels-v1'
    ),
    policy_id TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    schedule_fingerprint TEXT NOT NULL,
    calendar_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    risk_limit_policy_id TEXT NOT NULL,
    mandate_id TEXT NOT NULL,
    mandate_fingerprint TEXT NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    latest_expected_session DATE NOT NULL,
    schedule_checkpoint DATE,
    expected_constituent_count INTEGER NOT NULL CHECK (
        expected_constituent_count BETWEEN 1 AND 50
    ),
    freshness_exceptions JSONB NOT NULL,
    notification_retry_exhausted_events JSONB NOT NULL,
    maximum_notification_attempts INTEGER NOT NULL CHECK (
        maximum_notification_attempts BETWEEN 1 AND 10
    ),
    overall_status TEXT NOT NULL CHECK (
        overall_status IN ('ok', 'warning', 'critical')
    ),
    metrics_json JSONB NOT NULL,
    provider_request_performed BOOLEAN NOT NULL,
    external_delivery_performed BOOLEAN NOT NULL,
    cloud_schedule_activated BOOLEAN NOT NULL,
    report_json JSONB NOT NULL,
    document_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        calculation_id ~ '^operational-service-levels-v1-report-[0-9a-f]{24}$'
    ),
    CHECK (policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        policy_fingerprint ~ '^operational-slo-policy-[0-9a-f]{24}$'
    ),
    CHECK (schedule_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (schedule_fingerprint <> ''),
    CHECK (calendar_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (portfolio_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        risk_limit_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (mandate_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (mandate_fingerprint <> ''),
    CHECK (
        schedule_checkpoint IS NULL
        OR schedule_checkpoint <= latest_expected_session
    ),
    CHECK (jsonb_typeof(freshness_exceptions) = 'array'),
    CHECK (jsonb_typeof(notification_retry_exhausted_events) = 'array'),
    CHECK (jsonb_typeof(metrics_json) = 'array'),
    CHECK (jsonb_array_length(metrics_json) = 4),
    CHECK (jsonb_typeof(report_json) = 'object'),
    CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (NOT provider_request_performed),
    CHECK (NOT external_delivery_performed),
    CHECK (NOT cloud_schedule_activated),
    CHECK (report_json ->> 'calculation_id' = calculation_id),
    CHECK (report_json ->> 'model_version' = model_version),
    CHECK (report_json ->> 'policy_id' = policy_id),
    CHECK (report_json ->> 'policy_fingerprint' = policy_fingerprint),
    CHECK (report_json ->> 'schedule_id' = schedule_id),
    CHECK (report_json ->> 'schedule_fingerprint' = schedule_fingerprint),
    CHECK (report_json ->> 'calendar_id' = calendar_id),
    CHECK (report_json ->> 'portfolio_id' = portfolio_id),
    CHECK (report_json ->> 'risk_limit_policy_id' = risk_limit_policy_id),
    CHECK (report_json ->> 'mandate_id' = mandate_id),
    CHECK (report_json ->> 'mandate_fingerprint' = mandate_fingerprint),
    CHECK ((report_json ->> 'as_of')::TIMESTAMPTZ = as_of),
    CHECK (
        (report_json ->> 'latest_expected_session')::DATE
            = latest_expected_session
    ),
    CHECK (
        CASE
            WHEN report_json -> 'schedule_checkpoint' = 'null'::JSONB
                THEN schedule_checkpoint IS NULL
            ELSE (report_json ->> 'schedule_checkpoint')::DATE
                = schedule_checkpoint
        END
    ),
    CHECK (
        (report_json ->> 'expected_constituent_count')::INTEGER
            = expected_constituent_count
    ),
    CHECK (
        report_json -> 'freshness_exceptions' = freshness_exceptions
    ),
    CHECK (
        report_json -> 'notification_retry_exhausted_events'
            = notification_retry_exhausted_events
    ),
    CHECK (
        (report_json ->> 'maximum_notification_attempts')::INTEGER
            = maximum_notification_attempts
    ),
    CHECK (report_json ->> 'overall_status' = overall_status),
    CHECK (report_json -> 'metrics' = metrics_json),
    CHECK (
        (report_json ->> 'provider_request_performed')::BOOLEAN
            = provider_request_performed
    ),
    CHECK (
        (report_json ->> 'external_delivery_performed')::BOOLEAN
            = external_delivery_performed
    ),
    CHECK (
        (report_json ->> 'cloud_schedule_activated')::BOOLEAN
            = cloud_schedule_activated
    )
);

CREATE INDEX IF NOT EXISTS idx_operational_service_level_reports_latest
    ON risk_platform.operational_service_level_reports (
        policy_id,
        policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        portfolio_id,
        mandate_fingerprint,
        as_of DESC,
        calculation_id DESC
    );

CREATE OR REPLACE FUNCTION
    risk_platform.prevent_operational_service_level_report_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'operational service-level reports are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS prevent_operational_service_level_report_update
    ON risk_platform.operational_service_level_reports;
CREATE TRIGGER prevent_operational_service_level_report_update
BEFORE UPDATE ON risk_platform.operational_service_level_reports
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.prevent_operational_service_level_report_mutation();

DROP TRIGGER IF EXISTS prevent_operational_service_level_report_delete
    ON risk_platform.operational_service_level_reports;
CREATE TRIGGER prevent_operational_service_level_report_delete
BEFORE DELETE ON risk_platform.operational_service_level_reports
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.prevent_operational_service_level_report_mutation();

CREATE OR REPLACE VIEW risk_platform.operational_service_level_metric_history AS
SELECT
    report.calculation_id AS report_calculation_id,
    report.model_version,
    report.policy_id,
    report.policy_fingerprint,
    report.schedule_id,
    report.schedule_fingerprint,
    report.calendar_id,
    report.portfolio_id,
    report.risk_limit_policy_id,
    report.mandate_id,
    report.mandate_fingerprint,
    report.as_of,
    report.latest_expected_session,
    report.schedule_checkpoint,
    report.overall_status,
    metric.ordinality::INTEGER AS metric_ordinal,
    metric.value ->> 'metric_name' AS metric_name,
    NULLIF(metric.value ->> 'observed_value', '')::DOUBLE PRECISION
        AS observed_value,
    metric.value ->> 'unit' AS unit,
    (metric.value ->> 'warning_threshold')::DOUBLE PRECISION
        AS warning_threshold,
    (metric.value ->> 'critical_threshold')::DOUBLE PRECISION
        AS critical_threshold,
    metric.value ->> 'status' AS metric_status,
    NULLIF(metric.value ->> 'reason', '') AS reason,
    report.recorded_at
FROM risk_platform.operational_service_level_reports report
CROSS JOIN LATERAL jsonb_array_elements(report.metrics_json)
    WITH ORDINALITY AS metric(value, ordinality);

CREATE OR REPLACE VIEW risk_platform.latest_operational_service_level_reports AS
SELECT
    calculation_id,
    model_version,
    policy_id,
    policy_fingerprint,
    schedule_id,
    schedule_fingerprint,
    calendar_id,
    portfolio_id,
    risk_limit_policy_id,
    mandate_id,
    mandate_fingerprint,
    as_of,
    latest_expected_session,
    schedule_checkpoint,
    expected_constituent_count,
    freshness_exceptions,
    notification_retry_exhausted_events,
    maximum_notification_attempts,
    overall_status,
    metrics_json,
    report_json,
    document_sha256,
    recorded_at
FROM (
    SELECT
        report.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                schedule_id,
                schedule_fingerprint,
                portfolio_id,
                mandate_fingerprint
            ORDER BY as_of DESC, calculation_id DESC
        ) AS report_rank
    FROM risk_platform.operational_service_level_reports report
) ranked
WHERE report_rank = 1;

CREATE OR REPLACE VIEW
    risk_platform.current_operational_service_level_metric_status AS
SELECT
    report.calculation_id AS report_calculation_id,
    report.model_version,
    report.policy_id,
    report.policy_fingerprint,
    report.schedule_id,
    report.schedule_fingerprint,
    report.calendar_id,
    report.portfolio_id,
    report.risk_limit_policy_id,
    report.mandate_id,
    report.mandate_fingerprint,
    report.as_of,
    report.latest_expected_session,
    report.schedule_checkpoint,
    report.overall_status,
    metric.ordinality::INTEGER AS metric_ordinal,
    metric.value ->> 'metric_name' AS metric_name,
    NULLIF(metric.value ->> 'observed_value', '')::DOUBLE PRECISION
        AS observed_value,
    metric.value ->> 'unit' AS unit,
    (metric.value ->> 'warning_threshold')::DOUBLE PRECISION
        AS warning_threshold,
    (metric.value ->> 'critical_threshold')::DOUBLE PRECISION
        AS critical_threshold,
    metric.value ->> 'status' AS metric_status,
    NULLIF(metric.value ->> 'reason', '') AS reason,
    report.recorded_at
FROM risk_platform.latest_operational_service_level_reports report
CROSS JOIN LATERAL jsonb_array_elements(report.metrics_json)
    WITH ORDINALITY AS metric(value, ordinality);

CREATE OR REPLACE VIEW
    risk_platform.current_operational_service_level_exceptions AS
SELECT *
FROM risk_platform.current_operational_service_level_metric_status
WHERE metric_status IN ('warning', 'critical');
