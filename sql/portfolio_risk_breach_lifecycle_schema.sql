-- PostgreSQL breach-lifecycle views over current portfolio risk-limit evaluations.
-- Apply after sql/portfolio_risk_limits_schema.sql.

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_metric_transitions AS
WITH ordered AS (
    SELECT
        evaluation.*,
        LAG(status) OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name
            ORDER BY ts_event, ts_ingest, calculation_id
        ) AS previous_status,
        LAG(calculation_id) OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name
            ORDER BY ts_event, ts_ingest, calculation_id
        ) AS previous_calculation_id,
        LAG(subject_key) OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name
            ORDER BY ts_event, ts_ingest, calculation_id
        ) AS previous_subject_key
    FROM risk_platform.latest_portfolio_risk_limit_evaluations evaluation
)
SELECT
    ordered.*,
    CASE
        WHEN previous_status IS NULL AND status = 'ok' THEN 'initial_ok'
        WHEN previous_status IS NULL AND status IN ('warning', 'critical')
            THEN 'opened'
        WHEN previous_status = 'ok' AND status IN ('warning', 'critical')
            THEN 'opened'
        WHEN previous_status = 'warning' AND status = 'critical'
            THEN 'escalated'
        WHEN previous_status = 'critical' AND status = 'warning'
            THEN 'deescalated'
        WHEN previous_status IN ('warning', 'critical') AND status = 'ok'
            THEN 'resolved'
        ELSE 'unchanged'
    END AS transition_type,
    CASE status
        WHEN 'critical' THEN 2
        WHEN 'warning' THEN 1
        ELSE 0
    END AS severity_rank,
    (
        previous_subject_key IS NOT NULL
        AND previous_subject_key <> subject_key
    ) AS subject_changed
FROM ordered;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_actionable_transitions AS
SELECT *
FROM risk_platform.portfolio_risk_limit_metric_transitions
WHERE transition_type IN (
    'opened',
    'escalated',
    'deescalated',
    'resolved'
);

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_breach_episodes AS
WITH numbered AS (
    SELECT
        transition.*,
        SUM(
            CASE WHEN transition_type = 'opened' THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name
            ORDER BY ts_event, ts_ingest, calculation_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS episode_sequence
    FROM risk_platform.portfolio_risk_limit_metric_transitions transition
),
breach_rows AS (
    SELECT *
    FROM numbered
    WHERE is_breach
),
rollup AS (
    SELECT
        policy_id,
        policy_fingerprint,
        portfolio_id,
        base_currency,
        definition_fingerprint,
        model_version,
        attribution_model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        covariance_window,
        annualization_days,
        metric_name,
        subject_type,
        unit,
        warning_threshold,
        critical_threshold,
        episode_sequence,
        MIN(ts_event) AS episode_start,
        MAX(ts_event) AS last_breach_date,
        COUNT(*) AS breach_observations,
        COUNT(*) FILTER (WHERE status = 'warning') AS warning_observations,
        COUNT(*) FILTER (WHERE status = 'critical') AS critical_observations,
        COUNT(*) FILTER (WHERE subject_changed) AS subject_change_observations
    FROM breach_rows
    GROUP BY
        policy_id,
        policy_fingerprint,
        portfolio_id,
        base_currency,
        definition_fingerprint,
        model_version,
        attribution_model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        covariance_window,
        annualization_days,
        metric_name,
        subject_type,
        unit,
        warning_threshold,
        critical_threshold,
        episode_sequence
),
opening_rows AS (
    SELECT
        breach_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name,
                episode_sequence
            ORDER BY ts_event, ts_ingest, calculation_id
        ) AS row_rank
    FROM breach_rows
),
latest_breach_rows AS (
    SELECT
        breach_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name,
                episode_sequence
            ORDER BY ts_event DESC, ts_ingest DESC, calculation_id DESC
        ) AS row_rank
    FROM breach_rows
),
peak_rows AS (
    SELECT
        breach_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name,
                episode_sequence
            ORDER BY
                severity_rank DESC,
                breach_excess DESC,
                observed_value DESC,
                ts_event,
                ts_ingest,
                calculation_id
        ) AS row_rank
    FROM breach_rows
),
resolution_rows AS (
    SELECT
        numbered.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                policy_id,
                policy_fingerprint,
                portfolio_id,
                definition_fingerprint,
                model_version,
                attribution_model_version,
                weighting_method,
                covariance_method,
                correlation_method,
                covariance_window,
                annualization_days,
                metric_name,
                episode_sequence
            ORDER BY ts_event, ts_ingest, calculation_id
        ) AS row_rank
    FROM numbered
    WHERE status = 'ok' AND episode_sequence > 0
)
SELECT
    (
        rollup.policy_fingerprint
        || '|'
        || rollup.definition_fingerprint
        || '|'
        || rollup.metric_name
        || '|'
        || CAST(rollup.episode_start AS TEXT)
    ) AS episode_key,
    rollup.policy_id,
    rollup.policy_fingerprint,
    rollup.portfolio_id,
    rollup.base_currency,
    rollup.definition_fingerprint,
    rollup.model_version,
    rollup.attribution_model_version,
    rollup.weighting_method,
    rollup.covariance_method,
    rollup.correlation_method,
    rollup.covariance_window,
    rollup.annualization_days,
    rollup.metric_name,
    rollup.subject_type,
    rollup.unit,
    rollup.warning_threshold,
    rollup.critical_threshold,
    rollup.episode_sequence,
    rollup.episode_start,
    rollup.last_breach_date,
    resolution.ts_event AS resolution_date,
    CASE
        WHEN resolution.calculation_id IS NULL THEN 'open'
        ELSE 'resolved'
    END AS episode_status,
    rollup.breach_observations,
    rollup.warning_observations,
    rollup.critical_observations,
    rollup.subject_change_observations,
    opening.status AS opening_status,
    opening.subject_key AS opening_subject_key,
    opening.observed_value AS opening_observed_value,
    opening.calculation_id AS opening_evaluation_calculation_id,
    latest.status AS latest_breach_status,
    latest.subject_key AS latest_breach_subject_key,
    latest.observed_value AS latest_breach_observed_value,
    latest.breach_excess AS latest_breach_excess,
    latest.calculation_id AS last_breach_evaluation_calculation_id,
    peak.status AS peak_status,
    peak.subject_key AS peak_subject_key,
    peak.observed_value AS peak_observed_value,
    peak.observed_signed_value AS peak_observed_signed_value,
    peak.breach_excess AS peak_breach_excess,
    peak.calculation_id AS peak_evaluation_calculation_id,
    resolution.calculation_id AS resolution_evaluation_calculation_id,
    resolution.ts_ingest AS resolution_calculation_ts,
    CASE
        WHEN resolution.calculation_id IS NULL THEN latest.ts_ingest
        ELSE resolution.ts_ingest
    END AS latest_evidence_ts
FROM rollup
JOIN opening_rows opening
  ON opening.policy_id = rollup.policy_id
 AND opening.policy_fingerprint = rollup.policy_fingerprint
 AND opening.portfolio_id = rollup.portfolio_id
 AND opening.definition_fingerprint = rollup.definition_fingerprint
 AND opening.model_version = rollup.model_version
 AND opening.attribution_model_version = rollup.attribution_model_version
 AND opening.weighting_method = rollup.weighting_method
 AND opening.covariance_method = rollup.covariance_method
 AND opening.correlation_method = rollup.correlation_method
 AND opening.covariance_window = rollup.covariance_window
 AND opening.annualization_days = rollup.annualization_days
 AND opening.metric_name = rollup.metric_name
 AND opening.episode_sequence = rollup.episode_sequence
 AND opening.row_rank = 1
JOIN latest_breach_rows latest
  ON latest.policy_id = rollup.policy_id
 AND latest.policy_fingerprint = rollup.policy_fingerprint
 AND latest.portfolio_id = rollup.portfolio_id
 AND latest.definition_fingerprint = rollup.definition_fingerprint
 AND latest.model_version = rollup.model_version
 AND latest.attribution_model_version = rollup.attribution_model_version
 AND latest.weighting_method = rollup.weighting_method
 AND latest.covariance_method = rollup.covariance_method
 AND latest.correlation_method = rollup.correlation_method
 AND latest.covariance_window = rollup.covariance_window
 AND latest.annualization_days = rollup.annualization_days
 AND latest.metric_name = rollup.metric_name
 AND latest.episode_sequence = rollup.episode_sequence
 AND latest.row_rank = 1
JOIN peak_rows peak
  ON peak.policy_id = rollup.policy_id
 AND peak.policy_fingerprint = rollup.policy_fingerprint
 AND peak.portfolio_id = rollup.portfolio_id
 AND peak.definition_fingerprint = rollup.definition_fingerprint
 AND peak.model_version = rollup.model_version
 AND peak.attribution_model_version = rollup.attribution_model_version
 AND peak.weighting_method = rollup.weighting_method
 AND peak.covariance_method = rollup.covariance_method
 AND peak.correlation_method = rollup.correlation_method
 AND peak.covariance_window = rollup.covariance_window
 AND peak.annualization_days = rollup.annualization_days
 AND peak.metric_name = rollup.metric_name
 AND peak.episode_sequence = rollup.episode_sequence
 AND peak.row_rank = 1
LEFT JOIN resolution_rows resolution
  ON resolution.policy_id = rollup.policy_id
 AND resolution.policy_fingerprint = rollup.policy_fingerprint
 AND resolution.portfolio_id = rollup.portfolio_id
 AND resolution.definition_fingerprint = rollup.definition_fingerprint
 AND resolution.model_version = rollup.model_version
 AND resolution.attribution_model_version = rollup.attribution_model_version
 AND resolution.weighting_method = rollup.weighting_method
 AND resolution.covariance_method = rollup.covariance_method
 AND resolution.correlation_method = rollup.correlation_method
 AND resolution.covariance_window = rollup.covariance_window
 AND resolution.annualization_days = rollup.annualization_days
 AND resolution.metric_name = rollup.metric_name
 AND resolution.episode_sequence = rollup.episode_sequence
 AND resolution.row_rank = 1;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_open_episodes AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_episodes
WHERE episode_status = 'open';
