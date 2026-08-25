-- Current operational follow-up views for portfolio-risk notification delivery.
-- Apply after sql/portfolio_risk_notification_retry_execution_schema.sql.

CREATE OR REPLACE VIEW
risk_platform.notification_retry_execution_event_history AS
SELECT
    execution.record_id,
    execution.request_id,
    execution.execution_id,
    execution.plan_id,
    execution.terminal_status,
    execution.failure_code,
    execution.channel,
    execution.endpoint_host,
    execution.started_at,
    execution.finished_at,
    execution.recorded_at,
    execution.request_count,
    execution.attempts_persisted,
    requested.event_id,
    requested.event_ordinal::INTEGER AS event_ordinal,
    persisted.persisted_event_ordinal,
    persisted.persisted_attempt_id,
    persisted.persisted_event_id IS NOT NULL AS persisted_event_recorded,
    attempt.attempt_id IS NOT NULL AS attempt_evidence_confirmed,
    attempt.attempt_number,
    attempt.attempted_at,
    attempt.outcome AS attempt_outcome,
    attempt.http_status,
    attempt.error_code,
    attempt.payload_sha256
FROM risk_platform.portfolio_risk_notification_retry_executions execution
CROSS JOIN LATERAL
    jsonb_array_elements_text(execution.requested_event_ids_json)
    WITH ORDINALITY AS requested(event_id, event_ordinal)
LEFT JOIN LATERAL (
    SELECT
        persisted_event.event_id AS persisted_event_id,
        persisted_event.event_ordinal::INTEGER AS persisted_event_ordinal,
        persisted_attempt.attempt_id AS persisted_attempt_id
    FROM jsonb_array_elements_text(execution.persisted_event_ids_json)
        WITH ORDINALITY AS persisted_event(event_id, event_ordinal)
    LEFT JOIN jsonb_array_elements_text(execution.attempt_ids_json)
        WITH ORDINALITY AS persisted_attempt(attempt_id, attempt_ordinal)
      ON persisted_attempt.attempt_ordinal = persisted_event.event_ordinal
    WHERE persisted_event.event_id = requested.event_id
    ORDER BY persisted_event.event_ordinal
    LIMIT 1
) persisted ON TRUE
LEFT JOIN risk_platform.portfolio_risk_notification_delivery_attempts attempt
  ON attempt.attempt_id = persisted.persisted_attempt_id;

CREATE OR REPLACE VIEW
risk_platform.latest_notification_retry_execution_by_event AS
SELECT ranked.*
FROM (
    SELECT
        history.*,
        ROW_NUMBER() OVER (
            PARTITION BY history.event_id
            ORDER BY
                history.finished_at DESC,
                history.record_id DESC,
                history.event_ordinal DESC
        ) AS current_version_rank
    FROM risk_platform.notification_retry_execution_event_history history
) ranked
WHERE ranked.current_version_rank = 1;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_request_failures AS
SELECT
    execution.record_id,
    execution.request_id,
    execution.plan_id,
    execution.terminal_status,
    execution.failure_code,
    execution.started_at,
    execution.finished_at,
    execution.recorded_at,
    execution.request_count,
    execution.attempts_persisted,
    execution.requested_event_ids_json,
    execution.persisted_event_ids_json,
    CASE execution.terminal_status
        WHEN 'persistence_uncertain' THEN 'ambiguous_remote_outcome_review'
        WHEN 'failed_after_request' THEN 'post_request_review'
        ELSE 'pre_request_review'
    END AS review_type,
    CASE execution.terminal_status
        WHEN 'persistence_uncertain' THEN 3
        WHEN 'failed_after_request' THEN 2
        ELSE 1
    END AS review_priority
FROM risk_platform.portfolio_risk_notification_retry_executions execution
WHERE execution.terminal_status <> 'completed';

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_persistence_uncertainty AS
SELECT latest.*
FROM risk_platform.latest_notification_retry_execution_by_event latest
WHERE latest.terminal_status = 'persistence_uncertain'
  AND NOT latest.attempt_evidence_confirmed
  AND NOT EXISTS (
      SELECT 1
      FROM risk_platform.portfolio_risk_notification_delivery_attempts attempt
      WHERE attempt.event_id = latest.event_id
        AND attempt.channel = 'webhook'
        AND attempt.attempted_at >= latest.started_at
  );

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_follow_up AS
WITH evidence AS (
    SELECT
        pending.event_id,
        pending.event_type,
        pending.transition_type,
        pending.policy_id,
        pending.policy_fingerprint,
        pending.portfolio_id,
        pending.definition_fingerprint,
        pending.source_evaluation_calculation_id,
        pending.metric_name,
        pending.subject_type,
        pending.subject_key,
        pending.current_status,
        pending.ts_event,
        pending.ts_ingest,
        COALESCE(delivery.attempt_count, 0) AS attempt_count,
        COALESCE(delivery.failed_attempt_count, 0) AS failed_attempt_count,
        COALESCE(delivery.delivered, FALSE) AS delivered,
        latest_attempt.attempt_id AS latest_attempt_id,
        latest_attempt.attempt_number AS latest_attempt_number,
        latest_attempt.attempted_at AS latest_attempted_at,
        latest_attempt.outcome AS latest_attempt_outcome,
        latest_attempt.http_status AS latest_http_status,
        latest_attempt.error_code AS latest_error_code,
        latest_ack.acknowledgement_id,
        latest_ack.acknowledged_at,
        latest_ack.disposition AS acknowledgement_disposition,
        latest_execution.record_id AS latest_execution_record_id,
        latest_execution.request_id AS latest_execution_request_id,
        latest_execution.plan_id AS latest_execution_plan_id,
        latest_execution.terminal_status AS latest_execution_terminal_status,
        latest_execution.failure_code AS latest_execution_failure_code,
        latest_execution.finished_at AS latest_execution_finished_at,
        uncertainty.record_id AS uncertainty_record_id
    FROM risk_platform.portfolio_risk_notification_pending pending
    LEFT JOIN risk_platform.portfolio_risk_notification_delivery_status delivery
      ON delivery.event_id = pending.event_id
    LEFT JOIN LATERAL (
        SELECT
            attempt.attempt_id,
            attempt.attempt_number,
            attempt.attempted_at,
            attempt.outcome,
            attempt.http_status,
            attempt.error_code
        FROM risk_platform.portfolio_risk_notification_delivery_attempts attempt
        WHERE attempt.event_id = pending.event_id
          AND attempt.channel = 'webhook'
        ORDER BY
            attempt.attempt_number DESC,
            attempt.attempted_at DESC,
            attempt.attempt_id DESC
        LIMIT 1
    ) latest_attempt ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            acknowledgement.acknowledgement_id,
            acknowledgement.acknowledged_at,
            acknowledgement.disposition
        FROM risk_platform.portfolio_risk_limit_acknowledgements acknowledgement
        WHERE acknowledgement.evaluation_calculation_id
                = pending.source_evaluation_calculation_id
        ORDER BY
            acknowledgement.acknowledged_at DESC,
            acknowledgement.acknowledgement_id DESC
        LIMIT 1
    ) latest_ack ON TRUE
    LEFT JOIN risk_platform.latest_notification_retry_execution_by_event
        latest_execution
      ON latest_execution.event_id = pending.event_id
    LEFT JOIN risk_platform.current_notification_retry_persistence_uncertainty
        uncertainty
      ON uncertainty.event_id = pending.event_id
),
classified AS (
    SELECT
        evidence.*,
        CASE
            WHEN evidence.delivered THEN 'delivered'
            WHEN evidence.acknowledgement_id IS NOT NULL THEN 'acknowledged'
            WHEN evidence.uncertainty_record_id IS NOT NULL
                THEN 'persistence_review_required'
            WHEN evidence.attempt_count = 0 THEN 'initial_delivery_required'
            WHEN evidence.latest_execution_terminal_status IN (
                'failed_after_request',
                'persistence_uncertain'
            ) THEN 'execution_review_required'
            WHEN evidence.failed_attempt_count > 0 THEN 'retry_plan_required'
            ELSE 'review_required'
        END AS follow_up_reason
    FROM evidence
)
SELECT
    classified.*,
    classified.follow_up_reason NOT IN ('delivered', 'acknowledged')
        AS follow_up_required,
    classified.follow_up_reason IN (
        'persistence_review_required',
        'retry_plan_required',
        'execution_review_required',
        'review_required'
    ) AS delivery_failure,
    classified.follow_up_reason = 'persistence_review_required'
        AS ambiguous_outcome
FROM classified;

CREATE OR REPLACE VIEW
risk_platform.current_notification_delivery_failures AS
SELECT *
FROM risk_platform.current_notification_retry_follow_up
WHERE delivery_failure;

CREATE OR REPLACE VIEW
risk_platform.current_notification_ambiguous_outcomes AS
SELECT *
FROM risk_platform.current_notification_retry_follow_up
WHERE ambiguous_outcome;
