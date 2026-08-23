-- Append-only rolling operational service-level objective history and serving views.
-- Apply after sql/operational_service_levels_schema.sql so source report evidence exists.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.operational_service_level_objective_reports (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'operational-slo-attainment-v1'
    ),
    objective_policy_id TEXT NOT NULL,
    objective_policy_fingerprint TEXT NOT NULL,
    operational_policy_id TEXT NOT NULL,
    operational_policy_fingerprint TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    schedule_fingerprint TEXT NOT NULL,
    calendar_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    risk_limit_policy_id TEXT NOT NULL,
    mandate_id TEXT NOT NULL,
    mandate_fingerprint TEXT NOT NULL,
    through_session DATE NOT NULL,
    window_start_session DATE NOT NULL,
    window_end_session DATE NOT NULL,
    window_sessions INTEGER NOT NULL CHECK (
        window_sessions BETWEEN 2 AND 2520
    ),
    minimum_observations INTEGER NOT NULL CHECK (
        minimum_observations BETWEEN 2 AND window_sessions
    ),
    observations_available INTEGER NOT NULL CHECK (
        observations_available >= 0
    ),
    observations_expected INTEGER NOT NULL CHECK (
        observations_expected BETWEEN 1 AND window_sessions
    ),
    missing_report_sessions JSONB NOT NULL,
    window_complete BOOLEAN NOT NULL,
    history_status TEXT NOT NULL CHECK (
        history_status IN ('insufficient', 'ready')
    ),
    overall_status TEXT NOT NULL CHECK (
        overall_status IN ('insufficient', 'met', 'missed')
    ),
    calculated_at TIMESTAMPTZ NOT NULL,
    input_report_calculation_ids JSONB NOT NULL,
    input_report_document_sha256 JSONB NOT NULL,
    objectives_json JSONB NOT NULL,
    input_rows_scanned INTEGER NOT NULL CHECK (
        input_rows_scanned BETWEEN 0 AND 10000
    ),
    provider_request_performed BOOLEAN NOT NULL,
    external_delivery_performed BOOLEAN NOT NULL,
    cloud_schedule_activated BOOLEAN NOT NULL,
    automated_remediation_performed BOOLEAN NOT NULL,
    report_json JSONB NOT NULL,
    document_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        calculation_id
            ~ '^operational-slo-attainment-v1-report-[0-9a-f]{24}$'
    ),
    CHECK (
        objective_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (
        objective_policy_fingerprint
            ~ '^operational-slo-objective-policy-[0-9a-f]{24}$'
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
    CHECK (mandate_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (mandate_fingerprint <> ''),
    CHECK (window_start_session <= window_end_session),
    CHECK (window_end_session = through_session),
    CHECK (observations_available <= observations_expected),
    CHECK (input_rows_scanned >= observations_available),
    CHECK (jsonb_typeof(missing_report_sessions) = 'array'),
    CHECK (
        jsonb_array_length(missing_report_sessions)
            = observations_expected - observations_available
    ),
    CHECK (jsonb_typeof(input_report_calculation_ids) = 'array'),
    CHECK (
        jsonb_array_length(input_report_calculation_ids)
            = observations_available
    ),
    CHECK (jsonb_typeof(input_report_document_sha256) = 'array'),
    CHECK (
        jsonb_array_length(input_report_document_sha256)
            = observations_available
    ),
    CHECK (jsonb_typeof(objectives_json) = 'array'),
    CHECK (jsonb_array_length(objectives_json) = 4),
    CHECK (
        window_complete
            = (
                observations_expected = window_sessions
                AND jsonb_array_length(missing_report_sessions) = 0
            )
    ),
    CHECK (
        (history_status = 'insufficient'
            AND observations_available < minimum_observations)
        OR
        (history_status = 'ready'
            AND observations_available >= minimum_observations)
    ),
    CHECK (
        (history_status = 'insufficient'
            AND overall_status = 'insufficient')
        OR
        (history_status = 'ready'
            AND overall_status IN ('met', 'missed'))
    ),
    CHECK (NOT provider_request_performed),
    CHECK (NOT external_delivery_performed),
    CHECK (NOT cloud_schedule_activated),
    CHECK (NOT automated_remediation_performed),
    CHECK (jsonb_typeof(report_json) = 'object'),
    CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (report_json ->> 'calculation_id' = calculation_id),
    CHECK (report_json ->> 'model_version' = model_version),
    CHECK (report_json ->> 'objective_policy_id' = objective_policy_id),
    CHECK (
        report_json ->> 'objective_policy_fingerprint'
            = objective_policy_fingerprint
    ),
    CHECK (report_json ->> 'operational_policy_id' = operational_policy_id),
    CHECK (
        report_json ->> 'operational_policy_fingerprint'
            = operational_policy_fingerprint
    ),
    CHECK (report_json ->> 'schedule_id' = schedule_id),
    CHECK (report_json ->> 'schedule_fingerprint' = schedule_fingerprint),
    CHECK (report_json ->> 'calendar_id' = calendar_id),
    CHECK (report_json ->> 'portfolio_id' = portfolio_id),
    CHECK (report_json ->> 'risk_limit_policy_id' = risk_limit_policy_id),
    CHECK (report_json ->> 'mandate_id' = mandate_id),
    CHECK (report_json ->> 'mandate_fingerprint' = mandate_fingerprint),
    CHECK ((report_json ->> 'through_session')::DATE = through_session),
    CHECK (
        (report_json ->> 'window_start_session')::DATE
            = window_start_session
    ),
    CHECK (
        (report_json ->> 'window_end_session')::DATE = window_end_session
    ),
    CHECK (
        (report_json ->> 'window_sessions')::INTEGER = window_sessions
    ),
    CHECK (
        (report_json ->> 'minimum_observations')::INTEGER
            = minimum_observations
    ),
    CHECK (
        (report_json ->> 'observations_available')::INTEGER
            = observations_available
    ),
    CHECK (
        (report_json ->> 'observations_expected')::INTEGER
            = observations_expected
    ),
    CHECK (
        report_json -> 'missing_report_sessions' = missing_report_sessions
    ),
    CHECK (
        (report_json ->> 'window_complete')::BOOLEAN = window_complete
    ),
    CHECK (report_json ->> 'history_status' = history_status),
    CHECK (report_json ->> 'overall_status' = overall_status),
    CHECK ((report_json ->> 'calculated_at')::TIMESTAMPTZ = calculated_at),
    CHECK (
        report_json -> 'input_report_calculation_ids'
            = input_report_calculation_ids
    ),
    CHECK (
        report_json -> 'input_report_document_sha256'
            = input_report_document_sha256
    ),
    CHECK (report_json -> 'objectives' = objectives_json),
    CHECK (
        (report_json ->> 'input_rows_scanned')::INTEGER = input_rows_scanned
    )
);

CREATE INDEX IF NOT EXISTS idx_operational_slo_objective_reports_latest
    ON risk_platform.operational_service_level_objective_reports (
        objective_policy_id,
        objective_policy_fingerprint,
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        through_session DESC,
        calculated_at DESC,
        calculation_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_operational_slo_objective_inputs
    ON risk_platform.operational_service_level_objective_reports
    USING GIN (input_report_calculation_ids);

CREATE OR REPLACE FUNCTION
    risk_platform.prevent_operational_slo_objective_report_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'operational SLO objective reports are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS prevent_operational_slo_objective_report_update
    ON risk_platform.operational_service_level_objective_reports;
CREATE TRIGGER prevent_operational_slo_objective_report_update
BEFORE UPDATE ON risk_platform.operational_service_level_objective_reports
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.prevent_operational_slo_objective_report_mutation();

DROP TRIGGER IF EXISTS prevent_operational_slo_objective_report_delete
    ON risk_platform.operational_service_level_objective_reports;
CREATE TRIGGER prevent_operational_slo_objective_report_delete
BEFORE DELETE ON risk_platform.operational_service_level_objective_reports
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.prevent_operational_slo_objective_report_mutation();

CREATE OR REPLACE VIEW
    risk_platform.operational_service_level_objective_metric_history AS
SELECT
    report.calculation_id AS objective_report_calculation_id,
    report.model_version,
    report.objective_policy_id,
    report.objective_policy_fingerprint,
    report.operational_policy_id,
    report.operational_policy_fingerprint,
    report.schedule_id,
    report.schedule_fingerprint,
    report.calendar_id,
    report.portfolio_id,
    report.risk_limit_policy_id,
    report.mandate_id,
    report.mandate_fingerprint,
    report.through_session,
    report.window_start_session,
    report.window_end_session,
    report.window_sessions,
    report.minimum_observations,
    report.observations_available,
    report.observations_expected,
    report.window_complete,
    report.history_status,
    report.overall_status,
    report.calculated_at,
    objective.ordinality::INTEGER AS objective_ordinal,
    objective.value ->> 'objective_name' AS objective_name,
    objective.value ->> 'source_metric_name' AS source_metric_name,
    objective.value ->> 'source_unit' AS source_unit,
    (objective.value ->> 'success_threshold')::DOUBLE PRECISION
        AS success_threshold,
    (objective.value ->> 'target_ratio')::DOUBLE PRECISION
        AS target_ratio,
    (objective.value ->> 'successful_observations')::INTEGER
        AS successful_observations,
    (objective.value ->> 'failed_observations')::INTEGER
        AS failed_observations,
    (objective.value ->> 'missing_report_observations')::INTEGER
        AS missing_report_observations,
    (objective.value ->> 'observations_available')::INTEGER
        AS objective_observations_available,
    (objective.value ->> 'observations_expected')::INTEGER
        AS objective_observations_expected,
    (objective.value ->> 'attainment_ratio')::DOUBLE PRECISION
        AS attainment_ratio,
    objective.value ->> 'status' AS objective_status,
    report.recorded_at
FROM risk_platform.operational_service_level_objective_reports report
CROSS JOIN LATERAL jsonb_array_elements(report.objectives_json)
    WITH ORDINALITY AS objective(value, ordinality);

CREATE OR REPLACE VIEW
    risk_platform.latest_operational_service_level_objective_reports AS
SELECT
    calculation_id,
    model_version,
    objective_policy_id,
    objective_policy_fingerprint,
    operational_policy_id,
    operational_policy_fingerprint,
    schedule_id,
    schedule_fingerprint,
    calendar_id,
    portfolio_id,
    risk_limit_policy_id,
    mandate_id,
    mandate_fingerprint,
    through_session,
    window_start_session,
    window_end_session,
    window_sessions,
    minimum_observations,
    observations_available,
    observations_expected,
    missing_report_sessions,
    window_complete,
    history_status,
    overall_status,
    calculated_at,
    input_report_calculation_ids,
    input_report_document_sha256,
    objectives_json,
    input_rows_scanned,
    report_json,
    document_sha256,
    recorded_at
FROM (
    SELECT
        report.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                model_version,
                objective_policy_id,
                objective_policy_fingerprint,
                operational_policy_id,
                operational_policy_fingerprint,
                schedule_id,
                schedule_fingerprint,
                calendar_id,
                portfolio_id,
                risk_limit_policy_id,
                mandate_fingerprint,
                through_session
            ORDER BY calculated_at DESC, calculation_id DESC
        ) AS report_rank
    FROM risk_platform.operational_service_level_objective_reports report
) ranked
WHERE report_rank = 1;

CREATE OR REPLACE VIEW
    risk_platform.current_operational_service_level_objective_status AS
SELECT
    report.calculation_id AS objective_report_calculation_id,
    report.model_version,
    report.objective_policy_id,
    report.objective_policy_fingerprint,
    report.operational_policy_id,
    report.operational_policy_fingerprint,
    report.schedule_id,
    report.schedule_fingerprint,
    report.calendar_id,
    report.portfolio_id,
    report.risk_limit_policy_id,
    report.mandate_id,
    report.mandate_fingerprint,
    report.through_session,
    report.window_start_session,
    report.window_end_session,
    report.window_sessions,
    report.minimum_observations,
    report.observations_available,
    report.observations_expected,
    report.window_complete,
    report.history_status,
    report.overall_status,
    report.calculated_at,
    objective.ordinality::INTEGER AS objective_ordinal,
    objective.value ->> 'objective_name' AS objective_name,
    objective.value ->> 'source_metric_name' AS source_metric_name,
    objective.value ->> 'source_unit' AS source_unit,
    (objective.value ->> 'success_threshold')::DOUBLE PRECISION
        AS success_threshold,
    (objective.value ->> 'target_ratio')::DOUBLE PRECISION
        AS target_ratio,
    (objective.value ->> 'successful_observations')::INTEGER
        AS successful_observations,
    (objective.value ->> 'failed_observations')::INTEGER
        AS failed_observations,
    (objective.value ->> 'missing_report_observations')::INTEGER
        AS missing_report_observations,
    (objective.value ->> 'attainment_ratio')::DOUBLE PRECISION
        AS attainment_ratio,
    objective.value ->> 'status' AS objective_status,
    report.document_sha256,
    report.recorded_at
FROM risk_platform.latest_operational_service_level_objective_reports report
CROSS JOIN LATERAL jsonb_array_elements(report.objectives_json)
    WITH ORDINALITY AS objective(value, ordinality);

CREATE OR REPLACE VIEW
    risk_platform.current_operational_service_level_objective_exceptions AS
SELECT *
FROM risk_platform.current_operational_service_level_objective_status
WHERE objective_status IN ('missed', 'insufficient');
