-- Destination-aware operational views for current notification retry evidence.
-- Apply after retry destination bindings and retry follow-up views.

CREATE OR REPLACE VIEW
risk_platform.notification_retry_destination_execution_history AS
SELECT
    history.*,
    binding.binding_id,
    binding.authority_id AS destination_authority_id,
    binding.destination_id,
    binding.destination_fingerprint,
    binding.endpoint_environment_variable,
    binding.evaluated_at AS destination_evaluated_at,
    binding.recorded_at AS destination_binding_recorded_at,
    binding.evaluated_event_types_json,
    binding.record_id IS NOT NULL AS destination_bound
FROM risk_platform.notification_retry_execution_event_history history
LEFT JOIN
    risk_platform.portfolio_risk_notification_retry_destination_bindings binding
  ON binding.record_id = history.record_id;

CREATE OR REPLACE VIEW
risk_platform.latest_notification_retry_destination_by_event AS
SELECT
    latest.*,
    binding.binding_id,
    binding.authority_id AS destination_authority_id,
    binding.destination_id,
    binding.destination_fingerprint,
    binding.endpoint_environment_variable,
    binding.evaluated_at AS destination_evaluated_at,
    binding.recorded_at AS destination_binding_recorded_at,
    binding.evaluated_event_types_json,
    binding.record_id IS NOT NULL AS destination_bound
FROM risk_platform.latest_notification_retry_execution_by_event latest
LEFT JOIN
    risk_platform.portfolio_risk_notification_retry_destination_bindings binding
  ON binding.record_id = latest.record_id;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_destination_follow_up AS
SELECT
    follow_up.*,
    destination.binding_id,
    destination.destination_authority_id,
    destination.destination_id,
    destination.destination_fingerprint,
    destination.endpoint_environment_variable,
    destination.destination_evaluated_at,
    destination.destination_binding_recorded_at,
    destination.evaluated_event_types_json,
    COALESCE(destination.destination_bound, FALSE) AS destination_bound,
    CASE
        WHEN follow_up.latest_execution_record_id IS NULL
            THEN 'not_applicable'
        WHEN destination.destination_bound
            THEN 'bound'
        ELSE 'destination_binding_missing'
    END AS destination_binding_status,
    follow_up.latest_execution_record_id IS NOT NULL
        AND NOT COALESCE(destination.destination_bound, FALSE)
        AS destination_review_required
FROM risk_platform.current_notification_retry_follow_up follow_up
LEFT JOIN risk_platform.latest_notification_retry_destination_by_event destination
  ON destination.event_id = follow_up.event_id;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_destination_failures AS
SELECT *
FROM risk_platform.current_notification_retry_destination_follow_up
WHERE delivery_failure;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_destination_ambiguities AS
SELECT *
FROM risk_platform.current_notification_retry_destination_follow_up
WHERE ambiguous_outcome;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_destination_binding_reviews AS
SELECT *
FROM risk_platform.current_notification_retry_destination_follow_up
WHERE destination_review_required;
