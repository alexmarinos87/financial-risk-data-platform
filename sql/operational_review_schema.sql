-- Dashboard-ready, read-only operational review views.
-- Apply after operational service-level, objective and readiness decision schemas.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE OR REPLACE VIEW risk_platform.current_operational_health_summary AS
WITH contract_keys AS (
    SELECT DISTINCT
        policy_id AS operational_policy_id,
        policy_fingerprint AS operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session
    FROM risk_platform.latest_operational_service_level_reports
    UNION
    SELECT DISTINCT
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        through_session AS latest_expected_session
    FROM risk_platform.latest_operational_service_level_objective_reports
    UNION
    SELECT DISTINCT
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session
    FROM risk_platform.latest_operational_readiness_decisions
),
objective_rollup AS (
    SELECT
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        through_session AS latest_expected_session,
        MIN(mandate_id) AS mandate_id,
        COUNT(*)::INTEGER AS objective_policy_count,
        jsonb_agg(
            calculation_id
            ORDER BY objective_policy_id, objective_policy_fingerprint
        ) AS objective_report_calculation_ids,
        MAX(calculated_at) AS latest_objective_calculated_at,
        CASE MAX(
            CASE overall_status
                WHEN 'missed' THEN 2
                WHEN 'insufficient' THEN 1
                ELSE 0
            END
        )
            WHEN 2 THEN 'missed'
            WHEN 1 THEN 'insufficient'
            ELSE 'met'
        END AS objective_overall_status
    FROM risk_platform.latest_operational_service_level_objective_reports
    GROUP BY
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        through_session
),
metric_exceptions AS (
    SELECT
        policy_id AS operational_policy_id,
        policy_fingerprint AS operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session,
        COUNT(*)::INTEGER AS exception_count
    FROM risk_platform.current_operational_service_level_exceptions
    GROUP BY
        policy_id,
        policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session
),
objective_exceptions AS (
    SELECT
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        through_session AS latest_expected_session,
        COUNT(*)::INTEGER AS exception_count
    FROM risk_platform.current_operational_service_level_objective_exceptions
    GROUP BY
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        through_session
)
SELECT
    contract.operational_policy_id,
    contract.operational_policy_fingerprint,
    contract.schedule_id,
    contract.schedule_fingerprint,
    contract.calendar_id,
    contract.portfolio_id,
    contract.risk_limit_policy_id,
    COALESCE(report.mandate_id, objective.mandate_id) AS mandate_id,
    contract.mandate_fingerprint,
    contract.latest_expected_session,
    report.calculation_id AS service_level_report_calculation_id,
    report.document_sha256 AS service_level_report_document_sha256,
    report.as_of AS service_level_report_as_of,
    report.overall_status AS service_level_status,
    objective.objective_policy_count,
    objective.objective_report_calculation_ids,
    objective.latest_objective_calculated_at,
    objective.objective_overall_status,
    readiness.decision_id AS readiness_decision_id,
    readiness.document_sha256 AS readiness_document_sha256,
    readiness.evaluated_at AS readiness_evaluated_at,
    readiness.decision AS readiness_decision,
    readiness.reasons AS readiness_reasons,
    readiness.report_age_seconds AS gate_report_age_seconds,
    COALESCE(metric_exception.exception_count, 0)
        + COALESCE(objective_exception.exception_count, 0)
        + CASE
            WHEN readiness.decision = 'block'
                THEN jsonb_array_length(readiness.reasons)
            ELSE 0
          END AS current_exception_count,
    CASE
        WHEN readiness.decision = 'block' THEN 'blocked'
        WHEN report.overall_status = 'critical' THEN 'critical'
        WHEN objective.objective_overall_status = 'missed' THEN 'missed'
        WHEN readiness.decision IS NULL THEN 'readiness_missing'
        WHEN report.calculation_id IS NULL THEN 'service_level_missing'
        WHEN objective.objective_policy_count IS NULL THEN 'objective_missing'
        WHEN report.overall_status = 'warning' THEN 'warning'
        WHEN objective.objective_overall_status = 'insufficient'
            THEN 'insufficient'
        ELSE 'ok'
    END AS health_status
FROM contract_keys contract
LEFT JOIN risk_platform.latest_operational_service_level_reports report
    ON report.policy_id = contract.operational_policy_id
   AND report.policy_fingerprint = contract.operational_policy_fingerprint
   AND report.schedule_id = contract.schedule_id
   AND report.schedule_fingerprint = contract.schedule_fingerprint
   AND report.calendar_id = contract.calendar_id
   AND report.portfolio_id = contract.portfolio_id
   AND report.risk_limit_policy_id = contract.risk_limit_policy_id
   AND report.mandate_fingerprint = contract.mandate_fingerprint
   AND report.latest_expected_session = contract.latest_expected_session
LEFT JOIN objective_rollup objective
    ON objective.operational_policy_id = contract.operational_policy_id
   AND objective.operational_policy_fingerprint
        = contract.operational_policy_fingerprint
   AND objective.schedule_id = contract.schedule_id
   AND objective.schedule_fingerprint = contract.schedule_fingerprint
   AND objective.calendar_id = contract.calendar_id
   AND objective.portfolio_id = contract.portfolio_id
   AND objective.risk_limit_policy_id = contract.risk_limit_policy_id
   AND objective.mandate_fingerprint = contract.mandate_fingerprint
   AND objective.latest_expected_session = contract.latest_expected_session
LEFT JOIN risk_platform.latest_operational_readiness_decisions readiness
    ON readiness.operational_policy_id = contract.operational_policy_id
   AND readiness.operational_policy_fingerprint
        = contract.operational_policy_fingerprint
   AND readiness.schedule_id = contract.schedule_id
   AND readiness.schedule_fingerprint = contract.schedule_fingerprint
   AND readiness.calendar_id = contract.calendar_id
   AND readiness.portfolio_id = contract.portfolio_id
   AND readiness.risk_limit_policy_id = contract.risk_limit_policy_id
   AND readiness.mandate_fingerprint = contract.mandate_fingerprint
   AND readiness.latest_expected_session = contract.latest_expected_session
LEFT JOIN metric_exceptions metric_exception
    ON metric_exception.operational_policy_id = contract.operational_policy_id
   AND metric_exception.operational_policy_fingerprint
        = contract.operational_policy_fingerprint
   AND metric_exception.schedule_id = contract.schedule_id
   AND metric_exception.schedule_fingerprint = contract.schedule_fingerprint
   AND metric_exception.calendar_id = contract.calendar_id
   AND metric_exception.portfolio_id = contract.portfolio_id
   AND metric_exception.risk_limit_policy_id = contract.risk_limit_policy_id
   AND metric_exception.mandate_fingerprint = contract.mandate_fingerprint
   AND metric_exception.latest_expected_session
        = contract.latest_expected_session
LEFT JOIN objective_exceptions objective_exception
    ON objective_exception.operational_policy_id
        = contract.operational_policy_id
   AND objective_exception.operational_policy_fingerprint
        = contract.operational_policy_fingerprint
   AND objective_exception.schedule_id = contract.schedule_id
   AND objective_exception.schedule_fingerprint = contract.schedule_fingerprint
   AND objective_exception.calendar_id = contract.calendar_id
   AND objective_exception.portfolio_id = contract.portfolio_id
   AND objective_exception.risk_limit_policy_id
        = contract.risk_limit_policy_id
   AND objective_exception.mandate_fingerprint = contract.mandate_fingerprint
   AND objective_exception.latest_expected_session
        = contract.latest_expected_session;

CREATE OR REPLACE VIEW risk_platform.current_operational_exception_summary AS
SELECT
    'service_level_metric'::TEXT AS exception_type,
    metric.metric_name AS exception_name,
    metric.metric_status AS exception_status,
    CASE metric.metric_status WHEN 'critical' THEN 3 ELSE 2 END
        AS severity_rank,
    metric.policy_id AS operational_policy_id,
    metric.policy_fingerprint AS operational_policy_fingerprint,
    NULL::TEXT AS objective_policy_id,
    NULL::TEXT AS objective_policy_fingerprint,
    metric.schedule_id,
    metric.schedule_fingerprint,
    metric.calendar_id,
    metric.portfolio_id,
    metric.risk_limit_policy_id,
    metric.mandate_id,
    metric.mandate_fingerprint,
    metric.latest_expected_session AS event_session,
    metric.report_calculation_id AS evidence_id,
    metric.as_of AS evidence_ts,
    NULL::TEXT AS parent_evidence_id,
    metric.observed_value,
    metric.warning_threshold,
    metric.critical_threshold AS target_or_critical_threshold,
    metric.unit,
    metric.reason
FROM risk_platform.current_operational_service_level_exceptions metric

UNION ALL

SELECT
    'service_level_objective',
    objective.objective_name,
    objective.objective_status,
    CASE objective.objective_status WHEN 'missed' THEN 3 ELSE 1 END,
    objective.operational_policy_id,
    objective.operational_policy_fingerprint,
    objective.objective_policy_id,
    objective.objective_policy_fingerprint,
    objective.schedule_id,
    objective.schedule_fingerprint,
    objective.calendar_id,
    objective.portfolio_id,
    objective.risk_limit_policy_id,
    objective.mandate_id,
    objective.mandate_fingerprint,
    objective.through_session,
    objective.objective_report_calculation_id,
    objective.calculated_at,
    NULL::TEXT,
    objective.attainment_ratio,
    NULL::DOUBLE PRECISION,
    objective.target_ratio,
    'ratio'::TEXT,
    NULL::TEXT
FROM risk_platform.current_operational_service_level_objective_exceptions objective

UNION ALL

SELECT
    'readiness_reason',
    reason.value,
    'block',
    4,
    readiness.operational_policy_id,
    readiness.operational_policy_fingerprint,
    NULL::TEXT,
    NULL::TEXT,
    readiness.schedule_id,
    readiness.schedule_fingerprint,
    readiness.calendar_id,
    readiness.portfolio_id,
    readiness.risk_limit_policy_id,
    NULL::TEXT,
    readiness.mandate_fingerprint,
    readiness.latest_expected_session,
    readiness.decision_id,
    readiness.evaluated_at,
    readiness.report_calculation_id,
    NULL::DOUBLE PRECISION,
    NULL::DOUBLE PRECISION,
    NULL::DOUBLE PRECISION,
    'decision'::TEXT,
    reason.value
FROM risk_platform.current_blocked_operational_readiness_decisions readiness
CROSS JOIN LATERAL jsonb_array_elements_text(readiness.reasons) reason(value);

CREATE OR REPLACE VIEW risk_platform.recent_operational_readiness_decisions AS
SELECT
    history.*,
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
    ) AS decision_recency_rank
FROM risk_platform.operational_readiness_decision_history history;

CREATE OR REPLACE VIEW risk_platform.rolling_operational_objective_attainment AS
SELECT
    objective_report_calculation_id,
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
    window_complete,
    history_status,
    overall_status,
    calculated_at,
    objective_ordinal,
    objective_name,
    source_metric_name,
    source_unit,
    success_threshold,
    target_ratio,
    successful_observations,
    failed_observations,
    missing_report_observations,
    objective_observations_available,
    objective_observations_expected,
    attainment_ratio,
    attainment_ratio - target_ratio AS attainment_gap,
    CASE objective_status
        WHEN 'missed' THEN 2
        WHEN 'insufficient' THEN 1
        ELSE 0
    END AS objective_status_rank,
    objective_status,
    recorded_at
FROM risk_platform.operational_service_level_objective_metric_history;

CREATE OR REPLACE VIEW risk_platform.operational_evidence_drillthrough AS
SELECT
    'service_level_report'::TEXT AS evidence_type,
    report.model_version,
    report.calculation_id AS evidence_id,
    '[]'::JSONB AS parent_evidence_ids,
    report.latest_expected_session AS event_session,
    report.as_of AS evidence_ts,
    report.overall_status AS status,
    report.policy_id AS operational_policy_id,
    report.policy_fingerprint AS operational_policy_fingerprint,
    NULL::TEXT AS objective_policy_id,
    NULL::TEXT AS objective_policy_fingerprint,
    report.schedule_id,
    report.schedule_fingerprint,
    report.calendar_id,
    report.portfolio_id,
    report.risk_limit_policy_id,
    report.mandate_id,
    report.mandate_fingerprint,
    report.document_sha256
FROM risk_platform.operational_service_level_reports report

UNION ALL

SELECT
    'service_level_objective_report',
    report.model_version,
    report.calculation_id,
    report.input_report_calculation_ids,
    report.through_session,
    report.calculated_at,
    report.overall_status,
    report.operational_policy_id,
    report.operational_policy_fingerprint,
    report.objective_policy_id,
    report.objective_policy_fingerprint,
    report.schedule_id,
    report.schedule_fingerprint,
    report.calendar_id,
    report.portfolio_id,
    report.risk_limit_policy_id,
    report.mandate_id,
    report.mandate_fingerprint,
    report.document_sha256
FROM risk_platform.operational_service_level_objective_reports report

UNION ALL

SELECT
    'readiness_decision',
    decision.model_version,
    decision.decision_id,
    CASE
        WHEN decision.report_calculation_id IS NULL THEN '[]'::JSONB
        ELSE jsonb_build_array(decision.report_calculation_id)
    END,
    decision.latest_expected_session,
    decision.evaluated_at,
    decision.decision,
    decision.operational_policy_id,
    decision.operational_policy_fingerprint,
    NULL::TEXT,
    NULL::TEXT,
    decision.schedule_id,
    decision.schedule_fingerprint,
    decision.calendar_id,
    decision.portfolio_id,
    decision.risk_limit_policy_id,
    NULL::TEXT,
    decision.mandate_fingerprint,
    decision.document_sha256
FROM risk_platform.operational_readiness_decisions decision;
