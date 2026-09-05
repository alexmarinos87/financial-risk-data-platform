-- Readiness-aware operational views for current notification retry evidence.
-- Apply after destination-aware retry follow-up and readiness-binding history.

CREATE OR REPLACE VIEW
risk_platform.notification_retry_readiness_execution_history AS
SELECT
    history.*,
    readiness.binding_id AS readiness_binding_id,
    readiness.readiness_record_id,
    readiness.readiness_request_id,
    readiness.retained_decision_id,
    readiness.refreshed_decision_id,
    readiness.enforcement_id,
    readiness.destination_id AS readiness_destination_id,
    readiness.enforced_at AS readiness_enforced_at,
    readiness.retained_decision_evaluated_at,
    readiness.refreshed_decision_evaluated_at,
    readiness.lock_model_version AS readiness_lock_model_version,
    readiness.lock_scope AS readiness_lock_scope,
    readiness.lock_key_fingerprint AS readiness_lock_key_fingerprint,
    readiness.binding_recorded_at AS readiness_binding_recorded_at,
    readiness.terminal_document_sha256 AS readiness_terminal_document_sha256,
    readiness.readiness_enforcement_sha256,
    readiness.document_sha256 AS readiness_binding_document_sha256,
    readiness.terminal_record_id IS NOT NULL AS readiness_bound
FROM risk_platform.notification_retry_destination_execution_history history
LEFT JOIN risk_platform.notification_retry_readiness_bindings readiness
  ON readiness.terminal_record_id = history.record_id;

CREATE OR REPLACE VIEW
risk_platform.latest_notification_retry_readiness_by_event AS
SELECT
    latest.*,
    readiness.binding_id AS readiness_binding_id,
    readiness.readiness_record_id,
    readiness.readiness_request_id,
    readiness.retained_decision_id,
    readiness.refreshed_decision_id,
    readiness.enforcement_id,
    readiness.destination_id AS readiness_destination_id,
    readiness.enforced_at AS readiness_enforced_at,
    readiness.retained_decision_evaluated_at,
    readiness.refreshed_decision_evaluated_at,
    readiness.lock_model_version AS readiness_lock_model_version,
    readiness.lock_scope AS readiness_lock_scope,
    readiness.lock_key_fingerprint AS readiness_lock_key_fingerprint,
    readiness.binding_recorded_at AS readiness_binding_recorded_at,
    readiness.terminal_document_sha256 AS readiness_terminal_document_sha256,
    readiness.readiness_enforcement_sha256,
    readiness.document_sha256 AS readiness_binding_document_sha256,
    readiness.terminal_record_id IS NOT NULL AS readiness_bound
FROM risk_platform.latest_notification_retry_destination_by_event latest
LEFT JOIN risk_platform.notification_retry_readiness_bindings readiness
  ON readiness.terminal_record_id = latest.record_id;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_readiness_follow_up AS
SELECT
    follow_up.*,
    readiness.readiness_binding_id,
    readiness.readiness_record_id,
    readiness.readiness_request_id,
    readiness.retained_decision_id,
    readiness.refreshed_decision_id,
    readiness.enforcement_id,
    readiness.readiness_destination_id,
    readiness.readiness_enforced_at,
    readiness.retained_decision_evaluated_at,
    readiness.refreshed_decision_evaluated_at,
    readiness.readiness_lock_model_version,
    readiness.readiness_lock_scope,
    readiness.readiness_lock_key_fingerprint,
    readiness.readiness_binding_recorded_at,
    readiness.readiness_terminal_document_sha256,
    readiness.readiness_enforcement_sha256,
    readiness.readiness_binding_document_sha256,
    COALESCE(readiness.readiness_bound, FALSE) AS readiness_bound,
    CASE
        WHEN follow_up.latest_execution_record_id IS NULL THEN NULL
        WHEN NOT COALESCE(readiness.readiness_bound, FALSE) THEN NULL
        WHEN NOT follow_up.destination_bound THEN NULL
        ELSE readiness.readiness_destination_id = follow_up.destination_id
    END AS readiness_destination_matches,
    CASE
        WHEN follow_up.latest_execution_record_id IS NULL
            THEN 'not_applicable'
        WHEN NOT COALESCE(readiness.readiness_bound, FALSE)
            THEN 'readiness_binding_missing'
        WHEN follow_up.destination_bound
             AND readiness.readiness_destination_id <> follow_up.destination_id
            THEN 'readiness_destination_mismatch'
        ELSE 'bound'
    END AS readiness_binding_status,
    CASE
        WHEN follow_up.latest_execution_record_id IS NULL THEN FALSE
        WHEN NOT COALESCE(readiness.readiness_bound, FALSE) THEN TRUE
        WHEN follow_up.destination_bound
             AND readiness.readiness_destination_id <> follow_up.destination_id
            THEN TRUE
        ELSE FALSE
    END AS readiness_review_required
FROM risk_platform.current_notification_retry_destination_follow_up follow_up
LEFT JOIN risk_platform.latest_notification_retry_readiness_by_event readiness
  ON readiness.event_id = follow_up.event_id
 AND readiness.record_id = follow_up.latest_execution_record_id;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_readiness_failures AS
SELECT *
FROM risk_platform.current_notification_retry_readiness_follow_up
WHERE delivery_failure;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_readiness_ambiguities AS
SELECT *
FROM risk_platform.current_notification_retry_readiness_follow_up
WHERE ambiguous_outcome;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_readiness_binding_reviews AS
SELECT *
FROM risk_platform.current_notification_retry_readiness_follow_up
WHERE readiness_review_required;

CREATE OR REPLACE VIEW
risk_platform.current_notification_retry_readiness_bound AS
SELECT *
FROM risk_platform.current_notification_retry_readiness_follow_up
WHERE readiness_binding_status = 'bound';
