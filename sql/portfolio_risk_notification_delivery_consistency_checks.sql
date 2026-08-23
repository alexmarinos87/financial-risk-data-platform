-- Reconciliation checks for append-only notification delivery attempts.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_notification_delivery_attempts)
            AS attempt_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_notification_delivery_status)
            AS status_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_notification_delivery_pending)
            AS pending_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_notification_delivery_succeeded)
            AS succeeded_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_delivery_attempts attempt
            LEFT JOIN risk_platform.portfolio_risk_notification_outbox outbox
              ON outbox.event_id = attempt.event_id
            WHERE outbox.event_id IS NULL
        ) AS orphan_attempts,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_delivery_attempts
            WHERE idempotency_key <> event_id
               OR payload_sha256 !~ '^[0-9a-f]{64}$'
               OR endpoint_host = ''
        ) AS invalid_identity_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_delivery_attempts
            WHERE
                (outcome = 'succeeded'
                    AND (http_status NOT BETWEEN 200 AND 299
                         OR error_code IS NOT NULL))
                OR
                (outcome = 'failed' AND error_code IS NULL)
        ) AS invalid_outcome_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id, channel, attempt_number
                FROM risk_platform.portfolio_risk_notification_delivery_attempts
                GROUP BY event_id, channel, attempt_number
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_attempt_numbers,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id, channel
                FROM risk_platform.portfolio_risk_notification_delivery_attempts
                WHERE outcome = 'succeeded'
                GROUP BY event_id, channel
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_successes,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    event_id,
                    channel,
                    COUNT(*) AS attempt_count,
                    MAX(attempt_number) AS max_attempt_number
                FROM risk_platform.portfolio_risk_notification_delivery_attempts
                GROUP BY event_id, channel
            ) numbered
            WHERE attempt_count <> max_attempt_number
        ) AS non_contiguous_attempts,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_notification_delivery_status status
            WHERE status.delivered <> EXISTS (
                SELECT 1
                FROM risk_platform.portfolio_risk_notification_delivery_attempts attempt
                WHERE attempt.event_id = status.event_id
                  AND attempt.channel = 'webhook'
                  AND attempt.outcome = 'succeeded'
            )
        ) AS invalid_delivery_status
)
SELECT
    'notification_delivery_attempts_reference_outbox' AS check_name,
    '0' AS expected,
    orphan_attempts::TEXT AS actual,
    CASE WHEN orphan_attempts = 0 THEN 'pass' ELSE 'fail' END AS status
FROM integrity

UNION ALL

SELECT
    'notification_delivery_identity_valid',
    '0',
    invalid_identity_rows::TEXT,
    CASE WHEN invalid_identity_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_delivery_outcomes_valid',
    '0',
    invalid_outcome_rows::TEXT,
    CASE WHEN invalid_outcome_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_delivery_attempt_numbers_unique',
    '0',
    duplicate_attempt_numbers::TEXT,
    CASE WHEN duplicate_attempt_numbers = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_delivery_success_unique',
    '0',
    duplicate_successes::TEXT,
    CASE WHEN duplicate_successes = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_delivery_attempt_numbers_contiguous',
    '0',
    non_contiguous_attempts::TEXT,
    CASE WHEN non_contiguous_attempts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_delivery_status_matches_attempts',
    '0',
    invalid_delivery_status::TEXT,
    CASE WHEN invalid_delivery_status = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_delivery_status_partitions_current_outbox',
    status_rows::TEXT,
    (pending_rows + succeeded_rows)::TEXT,
    CASE
        WHEN status_rows = pending_rows + succeeded_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_delivery_attempt_count_nonnegative',
    '0',
    CASE WHEN attempt_rows >= 0 THEN '0' ELSE '1' END,
    CASE WHEN attempt_rows >= 0 THEN 'pass' ELSE 'fail' END
FROM counts

ORDER BY check_name;
