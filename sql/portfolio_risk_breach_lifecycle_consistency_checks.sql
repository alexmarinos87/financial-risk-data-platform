-- Reconciliation checks for current portfolio risk-limit transitions and episodes.
-- Run after sql/portfolio_risk_breach_lifecycle_schema.sql.

WITH counts AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_limit_evaluations
        ) AS latest_evaluation_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_metric_transitions
        ) AS transition_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_actionable_transitions
        ) AS actionable_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_metric_transitions
            WHERE transition_type IN (
                'opened',
                'escalated',
                'deescalated',
                'resolved'
            )
        ) AS expected_actionable_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_episodes
        ) AS episode_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_metric_transitions
            WHERE transition_type = 'opened'
        ) AS opened_transition_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_episodes
            WHERE episode_status = 'resolved'
        ) AS resolved_episode_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_metric_transitions
            WHERE transition_type = 'resolved'
        ) AS resolved_transition_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_open_episodes
        ) AS open_view_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_episodes
            WHERE episode_status = 'open'
        ) AS expected_open_rows
),
episode_order AS (
    SELECT
        episode.*,
        LAG(episode_status) OVER (
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
            ORDER BY episode_start, episode_sequence
        ) AS previous_episode_status,
        LAG(
            COALESCE(resolution_date, last_breach_date)
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
            ORDER BY episode_start, episode_sequence
        ) AS previous_episode_boundary
    FROM risk_platform.portfolio_risk_limit_breach_episodes episode
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_metric_transitions
            WHERE
                transition_type NOT IN (
                    'initial_ok',
                    'opened',
                    'escalated',
                    'deescalated',
                    'resolved',
                    'unchanged'
                )
                OR severity_rank NOT IN (0, 1, 2)
                OR (status = 'ok' AND severity_rank <> 0)
                OR (status = 'warning' AND severity_rank <> 1)
                OR (status = 'critical' AND severity_rank <> 2)
                OR (
                    transition_type = 'opened'
                    AND NOT is_breach
                )
                OR (
                    transition_type = 'resolved'
                    AND (status <> 'ok' OR is_breach)
                )
        ) AS invalid_transition_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_episodes
            WHERE
                breach_observations <= 0
                OR breach_observations
                    <> warning_observations + critical_observations
                OR warning_observations < 0
                OR critical_observations < 0
                OR subject_change_observations < 0
                OR opening_status NOT IN ('warning', 'critical')
                OR latest_breach_status NOT IN ('warning', 'critical')
                OR peak_status NOT IN ('warning', 'critical')
                OR episode_start > last_breach_date
                OR peak_breach_excess < 0
                OR latest_breach_excess < 0
        ) AS invalid_episode_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_episodes
            WHERE
                (
                    episode_status = 'open'
                    AND (
                        resolution_date IS NOT NULL
                        OR resolution_evaluation_calculation_id IS NOT NULL
                        OR resolution_calculation_ts IS NOT NULL
                    )
                )
                OR (
                    episode_status = 'resolved'
                    AND (
                        resolution_date IS NULL
                        OR resolution_evaluation_calculation_id IS NULL
                        OR resolution_calculation_ts IS NULL
                        OR resolution_date <= last_breach_date
                    )
                )
                OR episode_status NOT IN ('open', 'resolved')
        ) AS invalid_episode_resolution_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_episodes episode
            LEFT JOIN
                risk_platform.portfolio_risk_limit_metric_transitions opening
                ON opening.calculation_id
                    = episode.opening_evaluation_calculation_id
            LEFT JOIN
                risk_platform.portfolio_risk_limit_metric_transitions latest
                ON latest.calculation_id
                    = episode.last_breach_evaluation_calculation_id
            LEFT JOIN
                risk_platform.portfolio_risk_limit_metric_transitions peak
                ON peak.calculation_id
                    = episode.peak_evaluation_calculation_id
            LEFT JOIN
                risk_platform.portfolio_risk_limit_metric_transitions resolution
                ON resolution.calculation_id
                    = episode.resolution_evaluation_calculation_id
            WHERE
                opening.calculation_id IS NULL
                OR opening.transition_type <> 'opened'
                OR NOT opening.is_breach
                OR latest.calculation_id IS NULL
                OR NOT latest.is_breach
                OR peak.calculation_id IS NULL
                OR NOT peak.is_breach
                OR (
                    episode.episode_status = 'resolved'
                    AND (
                        resolution.calculation_id IS NULL
                        OR resolution.transition_type <> 'resolved'
                        OR resolution.status <> 'ok'
                        OR resolution.is_breach
                    )
                )
        ) AS invalid_episode_boundary_references,
        (
            SELECT COUNT(*) - COUNT(DISTINCT episode_key)
            FROM risk_platform.portfolio_risk_limit_breach_episodes
        ) AS duplicate_episode_keys,
        (
            SELECT COUNT(*)
            FROM episode_order
            WHERE
                previous_episode_status = 'open'
                OR (
                    previous_episode_boundary IS NOT NULL
                    AND episode_start <= previous_episode_boundary
                )
        ) AS overlapping_episode_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_open_episodes episode
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.portfolio_risk_limit_metric_transitions later
                WHERE later.policy_id = episode.policy_id
                  AND later.policy_fingerprint = episode.policy_fingerprint
                  AND later.portfolio_id = episode.portfolio_id
                  AND later.definition_fingerprint
                        = episode.definition_fingerprint
                  AND later.model_version = episode.model_version
                  AND later.attribution_model_version
                        = episode.attribution_model_version
                  AND later.weighting_method = episode.weighting_method
                  AND later.covariance_method = episode.covariance_method
                  AND later.correlation_method = episode.correlation_method
                  AND later.covariance_window = episode.covariance_window
                  AND later.annualization_days = episode.annualization_days
                  AND later.metric_name = episode.metric_name
                  AND later.ts_event > episode.last_breach_date
            )
        ) AS stale_open_episode_rows
)
SELECT
    'portfolio_risk_limit_transition_rows_match_latest' AS check_name,
    latest_evaluation_rows::TEXT AS expected,
    transition_rows::TEXT AS actual,
    CASE
        WHEN latest_evaluation_rows = transition_rows THEN 'pass'
        ELSE 'fail'
    END AS status
FROM counts

UNION ALL

SELECT
    'portfolio_risk_limit_actionable_transition_rows_match',
    expected_actionable_rows::TEXT,
    actionable_rows::TEXT,
    CASE
        WHEN expected_actionable_rows = actionable_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'portfolio_risk_limit_opened_transitions_match_episodes',
    opened_transition_rows::TEXT,
    episode_rows::TEXT,
    CASE
        WHEN opened_transition_rows = episode_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'portfolio_risk_limit_resolutions_match_resolved_episodes',
    resolved_transition_rows::TEXT,
    resolved_episode_rows::TEXT,
    CASE
        WHEN resolved_transition_rows = resolved_episode_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'portfolio_risk_limit_open_episode_view_matches',
    expected_open_rows::TEXT,
    open_view_rows::TEXT,
    CASE WHEN expected_open_rows = open_view_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'portfolio_risk_limit_transition_contract_valid',
    '0',
    invalid_transition_rows::TEXT,
    CASE WHEN invalid_transition_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_risk_limit_episode_counts_reconcile',
    '0',
    invalid_episode_counts::TEXT,
    CASE WHEN invalid_episode_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_risk_limit_episode_resolution_reconciles',
    '0',
    invalid_episode_resolution_rows::TEXT,
    CASE
        WHEN invalid_episode_resolution_rows = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'portfolio_risk_limit_episode_boundaries_reference_transitions',
    '0',
    invalid_episode_boundary_references::TEXT,
    CASE
        WHEN invalid_episode_boundary_references = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'portfolio_risk_limit_episode_keys_unique',
    '0',
    duplicate_episode_keys::TEXT,
    CASE WHEN duplicate_episode_keys = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_risk_limit_episodes_do_not_overlap',
    '0',
    overlapping_episode_rows::TEXT,
    CASE WHEN overlapping_episode_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_risk_limit_open_episodes_are_current',
    '0',
    stale_open_episode_rows::TEXT,
    CASE WHEN stale_open_episode_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
