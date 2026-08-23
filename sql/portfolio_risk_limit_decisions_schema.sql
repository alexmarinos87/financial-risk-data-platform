-- Append-only operator decision serving for deterministic risk-limit notifications.
-- Apply after sql/portfolio_risk_limit_notifications_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.portfolio_risk_limit_decisions (
    decision_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    notification_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_risk_limit_notifications (notification_id),
    decision TEXT NOT NULL CHECK (
        decision IN ('acknowledged', 'resolved', 'waived')
    ),
    actor TEXT NOT NULL CHECK (
        actor <> '' AND char_length(actor) <= 128
    ),
    reason TEXT NOT NULL CHECK (
        reason <> '' AND char_length(reason) <= 1000
    ),
    decided_at TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    record_json JSONB NOT NULL CHECK (jsonb_typeof(record_json) = 'object'),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (decision_id <> ''),
    CHECK (model_version <> ''),
    CHECK (notification_id <> ''),
    CHECK (ts_ingest >= decided_at)
);

CREATE INDEX IF NOT EXISTS idx_risk_limit_decision_notification_order
    ON risk_platform.portfolio_risk_limit_decisions (
        notification_id,
        decided_at DESC,
        decision_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_risk_limit_decision_type_order
    ON risk_platform.portfolio_risk_limit_decisions (
        decision,
        decided_at DESC
    );

CREATE OR REPLACE VIEW risk_platform.latest_portfolio_risk_limit_decisions AS
SELECT
    decision_id,
    model_version,
    notification_id,
    decision,
    actor,
    reason,
    decided_at,
    ts_ingest,
    record_json
FROM (
    SELECT
        decision_record.*,
        ROW_NUMBER() OVER (
            PARTITION BY notification_id
            ORDER BY decided_at DESC, decision_id DESC
        ) AS version_rank
    FROM risk_platform.portfolio_risk_limit_decisions decision_record
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_breach_lifecycle AS
SELECT
    notification.*,
    decision_record.decision_id AS latest_decision_id,
    decision_record.model_version AS latest_decision_model_version,
    decision_record.decision AS latest_decision,
    decision_record.actor AS latest_decision_actor,
    decision_record.reason AS latest_decision_reason,
    decision_record.decided_at AS latest_decided_at,
    CASE
        WHEN decision_record.decision = 'resolved' THEN 'resolved'
        WHEN decision_record.decision = 'waived' THEN 'waived'
        WHEN decision_record.decision = 'acknowledged' THEN 'acknowledged'
        ELSE 'open'
    END AS lifecycle_status,
    decision_record.decision IS NULL AS requires_operator_decision,
    decision_record.decision IN ('resolved', 'waived') AS operationally_closed
FROM risk_platform.current_portfolio_risk_limit_notifications notification
LEFT JOIN risk_platform.latest_portfolio_risk_limit_decisions decision_record
    ON decision_record.notification_id = notification.notification_id;

CREATE OR REPLACE VIEW risk_platform.open_portfolio_risk_limit_breaches AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_lifecycle
WHERE lifecycle_status = 'open';

CREATE OR REPLACE VIEW risk_platform.acknowledged_portfolio_risk_limit_breaches AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_lifecycle
WHERE lifecycle_status = 'acknowledged';

CREATE OR REPLACE VIEW risk_platform.resolved_portfolio_risk_limit_breaches AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_lifecycle
WHERE lifecycle_status = 'resolved';

CREATE OR REPLACE VIEW risk_platform.waived_portfolio_risk_limit_breaches AS
SELECT *
FROM risk_platform.portfolio_risk_limit_breach_lifecycle
WHERE lifecycle_status = 'waived';

CREATE OR REPLACE VIEW risk_platform.portfolio_risk_limit_decision_summary AS
SELECT
    lifecycle_status,
    COUNT(*) AS notification_count,
    COUNT(*) FILTER (WHERE requires_operator_decision) AS undecided_count,
    COUNT(*) FILTER (WHERE operationally_closed) AS closed_count,
    MAX(latest_decided_at) AS latest_decided_at
FROM risk_platform.portfolio_risk_limit_breach_lifecycle
GROUP BY lifecycle_status;
