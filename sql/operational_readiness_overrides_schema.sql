-- Append-only, time-bounded human override evidence for blocked readiness decisions.
-- Apply after sql/operational_readiness_decisions_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.operational_readiness_overrides (
    override_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'operational-readiness-override-v1'
    ),
    decision_id TEXT NOT NULL REFERENCES
        risk_platform.operational_readiness_decisions (decision_id),
    decision_document_sha256 TEXT NOT NULL,
    gate_id TEXT NOT NULL,
    gate_fingerprint TEXT NOT NULL,
    operational_policy_id TEXT NOT NULL,
    operational_policy_fingerprint TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    schedule_fingerprint TEXT NOT NULL,
    calendar_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    risk_limit_policy_id TEXT NOT NULL,
    mandate_fingerprint TEXT NOT NULL,
    latest_expected_session DATE NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    approved_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    approved_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        override_id ~ '^operational-readiness-override-v1-[0-9a-f]{24}$'
    ),
    CHECK (decision_document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (gate_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (gate_fingerprint <> ''),
    CHECK (
        operational_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (operational_policy_fingerprint <> ''),
    CHECK (schedule_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (schedule_fingerprint <> ''),
    CHECK (calendar_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (portfolio_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        risk_limit_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (mandate_fingerprint <> ''),
    CHECK (request_id <> '' AND length(request_id) <= 128),
    CHECK (expires_at > approved_at),
    CHECK (expires_at - approved_at <= INTERVAL '24 hours'),
    CHECK (approved_by <> '' AND length(approved_by) <= 320),
    CHECK (reason <> '' AND length(reason) <= 2000)
);

CREATE TABLE IF NOT EXISTS
    risk_platform.operational_readiness_override_revocations (
        revocation_id TEXT PRIMARY KEY,
        model_version TEXT NOT NULL CHECK (
            model_version = 'operational-readiness-override-revocation-v1'
        ),
        override_id TEXT NOT NULL REFERENCES
            risk_platform.operational_readiness_overrides (override_id),
        request_id TEXT NOT NULL UNIQUE,
        revoked_at TIMESTAMPTZ NOT NULL,
        revoked_by TEXT NOT NULL,
        reason TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (
            revocation_id
                ~ '^operational-readiness-override-revocation-v1-[0-9a-f]{24}$'
        ),
        CHECK (request_id <> '' AND length(request_id) <= 128),
        CHECK (revoked_by <> '' AND length(revoked_by) <= 320),
        CHECK (reason <> '' AND length(reason) <= 2000)
    );

CREATE INDEX IF NOT EXISTS idx_operational_readiness_overrides_current
    ON risk_platform.operational_readiness_overrides (
        decision_id,
        approved_at DESC,
        override_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_operational_readiness_override_revocations
    ON risk_platform.operational_readiness_override_revocations (
        override_id,
        revoked_at DESC,
        revocation_id DESC
    );

CREATE OR REPLACE FUNCTION
    risk_platform.validate_operational_readiness_override_target()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target risk_platform.operational_readiness_decisions%ROWTYPE;
BEGIN
    SELECT * INTO target
    FROM risk_platform.operational_readiness_decisions
    WHERE decision_id = NEW.decision_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'readiness override target does not exist'
            USING ERRCODE = '23503';
    END IF;
    IF target.decision <> 'block' THEN
        RAISE EXCEPTION 'readiness override target must be blocked'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.decision_document_sha256 <> target.document_sha256
        OR NEW.gate_id <> target.gate_id
        OR NEW.gate_fingerprint <> target.gate_fingerprint
        OR NEW.operational_policy_id <> target.operational_policy_id
        OR NEW.operational_policy_fingerprint
            <> target.operational_policy_fingerprint
        OR NEW.schedule_id <> target.schedule_id
        OR NEW.schedule_fingerprint <> target.schedule_fingerprint
        OR NEW.calendar_id <> target.calendar_id
        OR NEW.portfolio_id <> target.portfolio_id
        OR NEW.risk_limit_policy_id <> target.risk_limit_policy_id
        OR NEW.mandate_fingerprint <> target.mandate_fingerprint
        OR NEW.latest_expected_session <> target.latest_expected_session
    THEN
        RAISE EXCEPTION 'readiness override metadata does not match target'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.approved_at < target.evaluated_at THEN
        RAISE EXCEPTION 'readiness override predates target decision'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validate_operational_readiness_override_target
    ON risk_platform.operational_readiness_overrides;
CREATE TRIGGER validate_operational_readiness_override_target
BEFORE INSERT ON risk_platform.operational_readiness_overrides
FOR EACH ROW
EXECUTE FUNCTION risk_platform.validate_operational_readiness_override_target();

CREATE OR REPLACE FUNCTION
    risk_platform.validate_operational_readiness_override_revocation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_approved_at TIMESTAMPTZ;
BEGIN
    SELECT approved_at INTO target_approved_at
    FROM risk_platform.operational_readiness_overrides
    WHERE override_id = NEW.override_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'readiness override revocation target does not exist'
            USING ERRCODE = '23503';
    END IF;
    IF NEW.revoked_at < target_approved_at THEN
        RAISE EXCEPTION 'readiness override revocation predates approval'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validate_operational_readiness_override_revocation
    ON risk_platform.operational_readiness_override_revocations;
CREATE TRIGGER validate_operational_readiness_override_revocation
BEFORE INSERT ON risk_platform.operational_readiness_override_revocations
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.validate_operational_readiness_override_revocation();

CREATE OR REPLACE FUNCTION
    risk_platform.prevent_operational_readiness_override_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'operational readiness override evidence is append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS prevent_operational_readiness_override_update
    ON risk_platform.operational_readiness_overrides;
CREATE TRIGGER prevent_operational_readiness_override_update
BEFORE UPDATE ON risk_platform.operational_readiness_overrides
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_operational_readiness_override_mutation();

DROP TRIGGER IF EXISTS prevent_operational_readiness_override_delete
    ON risk_platform.operational_readiness_overrides;
CREATE TRIGGER prevent_operational_readiness_override_delete
BEFORE DELETE ON risk_platform.operational_readiness_overrides
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_operational_readiness_override_mutation();

DROP TRIGGER IF EXISTS prevent_operational_readiness_override_revocation_update
    ON risk_platform.operational_readiness_override_revocations;
CREATE TRIGGER prevent_operational_readiness_override_revocation_update
BEFORE UPDATE ON risk_platform.operational_readiness_override_revocations
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_operational_readiness_override_mutation();

DROP TRIGGER IF EXISTS prevent_operational_readiness_override_revocation_delete
    ON risk_platform.operational_readiness_override_revocations;
CREATE TRIGGER prevent_operational_readiness_override_revocation_delete
BEFORE DELETE ON risk_platform.operational_readiness_override_revocations
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_operational_readiness_override_mutation();

CREATE OR REPLACE VIEW risk_platform.operational_readiness_override_history AS
SELECT
    override.override_id AS event_id,
    override.model_version,
    'approved'::TEXT AS event_type,
    override.decision_id,
    override.override_id,
    override.request_id,
    override.approved_at AS event_at,
    override.approved_by AS actor,
    override.reason,
    override.expires_at,
    override.created_at
FROM risk_platform.operational_readiness_overrides override

UNION ALL

SELECT
    revocation.revocation_id,
    revocation.model_version,
    'revoked',
    override.decision_id,
    revocation.override_id,
    revocation.request_id,
    revocation.revoked_at,
    revocation.revoked_by,
    revocation.reason,
    override.expires_at,
    revocation.created_at
FROM risk_platform.operational_readiness_override_revocations revocation
JOIN risk_platform.operational_readiness_overrides override
    ON override.override_id = revocation.override_id;

CREATE OR REPLACE VIEW
    risk_platform.current_operational_readiness_override_status AS
SELECT
    override.override_id,
    override.model_version,
    override.decision_id,
    override.decision_document_sha256,
    override.gate_id,
    override.gate_fingerprint,
    override.operational_policy_id,
    override.operational_policy_fingerprint,
    override.schedule_id,
    override.schedule_fingerprint,
    override.calendar_id,
    override.portfolio_id,
    override.risk_limit_policy_id,
    override.mandate_fingerprint,
    override.latest_expected_session,
    override.request_id,
    override.approved_at,
    override.expires_at,
    override.approved_by,
    override.reason,
    revocation.revocation_id,
    revocation.request_id AS revocation_request_id,
    revocation.revoked_at,
    revocation.revoked_by,
    CASE
        WHEN override.approved_at > CURRENT_TIMESTAMP THEN 'pending'
        WHEN revocation.revoked_at IS NOT NULL
             AND revocation.revoked_at <= CURRENT_TIMESTAMP THEN 'revoked'
        WHEN override.expires_at <= CURRENT_TIMESTAMP THEN 'expired'
        ELSE 'active'
    END AS override_status,
    override.created_at
FROM (
    SELECT ranked.*
    FROM (
        SELECT
            candidate.*,
            ROW_NUMBER() OVER (
                PARTITION BY candidate.decision_id
                ORDER BY candidate.approved_at DESC, candidate.override_id DESC
            ) AS override_rank
        FROM risk_platform.operational_readiness_overrides candidate
    ) ranked
    WHERE ranked.override_rank = 1
) override
LEFT JOIN LATERAL (
    SELECT candidate.*
    FROM risk_platform.operational_readiness_override_revocations candidate
    WHERE candidate.override_id = override.override_id
    ORDER BY candidate.revoked_at DESC, candidate.revocation_id DESC
    LIMIT 1
) revocation ON TRUE;

CREATE OR REPLACE VIEW
    risk_platform.active_operational_readiness_overrides AS
SELECT *
FROM risk_platform.current_operational_readiness_override_status
WHERE override_status = 'active';
