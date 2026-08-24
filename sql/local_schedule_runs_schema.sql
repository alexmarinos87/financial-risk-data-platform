-- Append-only terminal evidence for readiness-authorised local schedule attempts.
-- Apply after sql/operational_readiness_overrides_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.local_schedule_runs (
    run_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (model_version = 'local-schedule-run-v1'),
    request_id TEXT NOT NULL UNIQUE,
    plan_id TEXT NOT NULL,
    authority_id TEXT NOT NULL,
    authority_type TEXT NOT NULL CHECK (
        authority_type IN ('gate_allow', 'active_override')
    ),
    schedule_id TEXT NOT NULL,
    schedule_fingerprint TEXT NOT NULL,
    calendar_id TEXT NOT NULL,
    calendar_fingerprint TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    risk_limit_policy_id TEXT NOT NULL,
    mandate_id TEXT NOT NULL,
    mandate_fingerprint TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    latest_expected_session DATE NOT NULL,
    readiness_decision_id TEXT NOT NULL REFERENCES
        risk_platform.operational_readiness_decisions (decision_id),
    readiness_document_sha256 TEXT NOT NULL,
    override_id TEXT NULL REFERENCES
        risk_platform.operational_readiness_overrides (override_id),
    authorized_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    run_status TEXT NOT NULL CHECK (run_status IN ('completed', 'failed')),
    checkpoint_before DATE NULL,
    checkpoint_after DATE NULL,
    selected_session_count INTEGER NOT NULL,
    started_session_count INTEGER NOT NULL,
    completed_session_count INTEGER NOT NULL,
    failed_session DATE NULL,
    failed_stage_index INTEGER NULL,
    failed_stage_name TEXT NULL,
    failure_code TEXT NULL,
    run_json JSONB NOT NULL,
    document_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (run_id ~ '^local-schedule-run-v1-run-[0-9a-f]{24}$'),
    CHECK (
        plan_id
            ~ '^readiness-aware-schedule-plan-v1-plan-[0-9a-f]{24}$'
    ),
    CHECK (
        authority_id
            ~ '^operational-readiness-execution-authority-v1-authority-[0-9a-f]{24}$'
    ),
    CHECK (
        readiness_decision_id
            ~ '^operational-readiness-gate-v1-decision-[0-9a-f]{24}$'
    ),
    CHECK (
        override_id IS NULL
        OR override_id ~ '^operational-readiness-override-v1-[0-9a-f]{24}$'
    ),
    CHECK (readiness_document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (request_id <> '' AND length(request_id) <= 128),
    CHECK (request_id = authority_id),
    CHECK (schedule_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (schedule_fingerprint <> ''),
    CHECK (calendar_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (calendar_fingerprint <> ''),
    CHECK (portfolio_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        risk_limit_policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CHECK (mandate_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (mandate_fingerprint <> ''),
    CHECK (latest_expected_session <= as_of_date),
    CHECK (authorized_at <= started_at AND started_at <= finished_at),
    CHECK (
        selected_session_count BETWEEN 1 AND 31
        AND started_session_count BETWEEN 0 AND selected_session_count
        AND completed_session_count BETWEEN 0 AND started_session_count
    ),
    CHECK (failed_stage_index IS NULL OR failed_stage_index >= 0),
    CHECK (
        (authority_type = 'gate_allow' AND override_id IS NULL)
        OR (authority_type = 'active_override' AND override_id IS NOT NULL)
    ),
    CHECK (
        (run_status = 'completed'
         AND completed_session_count = selected_session_count
         AND failed_session IS NULL
         AND failed_stage_index IS NULL
         AND failed_stage_name IS NULL
         AND failure_code IS NULL)
        OR
        (run_status = 'failed'
         AND failure_code IS NOT NULL)
    ),
    CHECK (jsonb_typeof(run_json) = 'object'),
    CHECK (jsonb_typeof(run_json -> 'sessions') = 'array'),
    CHECK (jsonb_array_length(run_json -> 'sessions') = selected_session_count),
    CHECK (run_json ->> 'run_id' = run_id),
    CHECK (run_json ->> 'model_version' = model_version),
    CHECK (run_json ->> 'request_id' = request_id),
    CHECK (run_json ->> 'plan_id' = plan_id),
    CHECK (run_json ->> 'authority_id' = authority_id),
    CHECK (run_json ->> 'authority_type' = authority_type),
    CHECK ((run_json ->> 'override_id') IS NOT DISTINCT FROM override_id),
    CHECK (run_json ->> 'schedule_id' = schedule_id),
    CHECK (run_json ->> 'schedule_fingerprint' = schedule_fingerprint),
    CHECK (run_json ->> 'calendar_id' = calendar_id),
    CHECK (run_json ->> 'calendar_fingerprint' = calendar_fingerprint),
    CHECK (run_json ->> 'portfolio_id' = portfolio_id),
    CHECK (run_json ->> 'risk_limit_policy_id' = risk_limit_policy_id),
    CHECK (run_json ->> 'mandate_id' = mandate_id),
    CHECK (run_json ->> 'mandate_fingerprint' = mandate_fingerprint),
    CHECK ((run_json ->> 'as_of_date')::DATE = as_of_date),
    CHECK (
        (run_json ->> 'latest_expected_session')::DATE
            = latest_expected_session
    ),
    CHECK (run_json ->> 'readiness_decision_id' = readiness_decision_id),
    CHECK (
        run_json ->> 'readiness_document_sha256'
            = readiness_document_sha256
    ),
    CHECK ((run_json ->> 'authorized_at')::TIMESTAMPTZ = authorized_at),
    CHECK ((run_json ->> 'started_at')::TIMESTAMPTZ = started_at),
    CHECK ((run_json ->> 'finished_at')::TIMESTAMPTZ = finished_at),
    CHECK (run_json ->> 'run_status' = run_status),
    CHECK (
        (run_json ->> 'checkpoint_before')::DATE
            IS NOT DISTINCT FROM checkpoint_before
    ),
    CHECK (
        (run_json ->> 'checkpoint_after')::DATE
            IS NOT DISTINCT FROM checkpoint_after
    ),
    CHECK (
        (run_json ->> 'failed_session')::DATE
            IS NOT DISTINCT FROM failed_session
    ),
    CHECK (
        (run_json ->> 'failed_stage_index')::INTEGER
            IS NOT DISTINCT FROM failed_stage_index
    ),
    CHECK (
        (run_json ->> 'failed_stage_name')
            IS NOT DISTINCT FROM failed_stage_name
    ),
    CHECK (
        (run_json ->> 'failure_code') IS NOT DISTINCT FROM failure_code
    ),
    CHECK (
        (run_json ->> 'selected_session_count')::INTEGER
            = selected_session_count
    ),
    CHECK (
        (run_json ->> 'started_session_count')::INTEGER
            = started_session_count
    ),
    CHECK (
        (run_json ->> 'completed_session_count')::INTEGER
            = completed_session_count
    ),
    CHECK ((run_json ->> 'provider_request_performed')::BOOLEAN = FALSE),
    CHECK ((run_json ->> 'notification_delivery_performed')::BOOLEAN = FALSE),
    CHECK ((run_json ->> 'cloud_schedule_activated')::BOOLEAN = FALSE)
);

CREATE INDEX IF NOT EXISTS idx_local_schedule_runs_recent
    ON risk_platform.local_schedule_runs (
        schedule_id,
        started_at DESC,
        run_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_local_schedule_runs_status
    ON risk_platform.local_schedule_runs (
        run_status,
        finished_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_local_schedule_runs_decision
    ON risk_platform.local_schedule_runs (
        readiness_decision_id,
        started_at DESC
    );

CREATE OR REPLACE FUNCTION risk_platform.validate_local_schedule_run_authority()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    decision_row risk_platform.operational_readiness_decisions%ROWTYPE;
    override_row risk_platform.operational_readiness_overrides%ROWTYPE;
BEGIN
    SELECT * INTO decision_row
    FROM risk_platform.operational_readiness_decisions
    WHERE decision_id = NEW.readiness_decision_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'local schedule run readiness decision does not exist'
            USING ERRCODE = '23503';
    END IF;
    IF NEW.readiness_document_sha256 <> decision_row.document_sha256
        OR NEW.schedule_id <> decision_row.schedule_id
        OR NEW.schedule_fingerprint <> decision_row.schedule_fingerprint
        OR NEW.calendar_id <> decision_row.calendar_id
        OR NEW.portfolio_id <> decision_row.portfolio_id
        OR NEW.risk_limit_policy_id <> decision_row.risk_limit_policy_id
        OR NEW.mandate_fingerprint <> decision_row.mandate_fingerprint
        OR NEW.latest_expected_session <> decision_row.latest_expected_session
    THEN
        RAISE EXCEPTION 'local schedule run metadata does not match readiness decision'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.authority_type = 'gate_allow' THEN
        IF decision_row.decision <> 'allow' OR NEW.override_id IS NOT NULL THEN
            RAISE EXCEPTION 'gate_allow run requires an allow decision without override'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF decision_row.decision <> 'block' OR NEW.override_id IS NULL THEN
            RAISE EXCEPTION 'active_override run requires a blocked decision and override'
                USING ERRCODE = '23514';
        END IF;
        SELECT * INTO override_row
        FROM risk_platform.operational_readiness_overrides
        WHERE override_id = NEW.override_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'local schedule run override does not exist'
                USING ERRCODE = '23503';
        END IF;
        IF override_row.decision_id <> NEW.readiness_decision_id
            OR override_row.decision_document_sha256
                <> NEW.readiness_document_sha256
            OR override_row.schedule_id <> NEW.schedule_id
            OR override_row.schedule_fingerprint <> NEW.schedule_fingerprint
            OR override_row.calendar_id <> NEW.calendar_id
            OR override_row.portfolio_id <> NEW.portfolio_id
            OR override_row.risk_limit_policy_id <> NEW.risk_limit_policy_id
            OR override_row.mandate_fingerprint <> NEW.mandate_fingerprint
            OR override_row.latest_expected_session
                <> NEW.latest_expected_session
        THEN
            RAISE EXCEPTION 'local schedule run override metadata does not match'
                USING ERRCODE = '23514';
        END IF;
        IF NOT (
            override_row.approved_at <= NEW.started_at
            AND NEW.started_at < override_row.expires_at
        ) THEN
            RAISE EXCEPTION 'local schedule run started outside override window'
                USING ERRCODE = '23514';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM risk_platform.operational_readiness_override_revocations revocation
            WHERE revocation.override_id = NEW.override_id
              AND revocation.revoked_at <= NEW.started_at
        ) THEN
            RAISE EXCEPTION 'local schedule run used a revoked override'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validate_local_schedule_run_authority
    ON risk_platform.local_schedule_runs;
CREATE TRIGGER validate_local_schedule_run_authority
BEFORE INSERT ON risk_platform.local_schedule_runs
FOR EACH ROW
EXECUTE FUNCTION risk_platform.validate_local_schedule_run_authority();

CREATE OR REPLACE FUNCTION risk_platform.prevent_local_schedule_run_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'local schedule run evidence is append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS prevent_local_schedule_run_update
    ON risk_platform.local_schedule_runs;
CREATE TRIGGER prevent_local_schedule_run_update
BEFORE UPDATE ON risk_platform.local_schedule_runs
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_local_schedule_run_mutation();

DROP TRIGGER IF EXISTS prevent_local_schedule_run_delete
    ON risk_platform.local_schedule_runs;
CREATE TRIGGER prevent_local_schedule_run_delete
BEFORE DELETE ON risk_platform.local_schedule_runs
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_local_schedule_run_mutation();

CREATE OR REPLACE VIEW risk_platform.local_schedule_run_session_history AS
SELECT
    run.run_id,
    run.request_id,
    run.plan_id,
    run.authority_id,
    run.authority_type,
    run.schedule_id,
    run.run_status,
    session.ordinality::INTEGER - 1 AS session_index,
    (session.value ->> 'session_date')::DATE AS session_date,
    session.value ->> 'mandate_id' AS mandate_id,
    session.value ->> 'mandate_fingerprint' AS mandate_fingerprint,
    session.value ->> 'status' AS session_status,
    (session.value ->> 'started_at')::TIMESTAMPTZ AS started_at,
    (session.value ->> 'finished_at')::TIMESTAMPTZ AS finished_at,
    (session.value ->> 'checkpoint_after')::DATE AS checkpoint_after,
    (session.value ->> 'failed_stage_index')::INTEGER AS failed_stage_index,
    session.value ->> 'failed_stage_name' AS failed_stage_name,
    session.value ->> 'failure_code' AS failure_code,
    jsonb_array_length(session.value -> 'stages') AS attempted_stage_count,
    run.recorded_at
FROM risk_platform.local_schedule_runs run
CROSS JOIN LATERAL jsonb_array_elements(run.run_json -> 'sessions')
    WITH ORDINALITY AS session(value, ordinality);

CREATE OR REPLACE VIEW risk_platform.local_schedule_run_stage_history AS
SELECT
    run.run_id,
    run.request_id,
    run.schedule_id,
    session.ordinality::INTEGER - 1 AS session_index,
    (session.value ->> 'session_date')::DATE AS session_date,
    stage.ordinality::INTEGER - 1 AS stage_index,
    stage.value ->> 'stage_name' AS stage_name,
    stage.value ->> 'status' AS stage_status,
    (stage.value ->> 'started_at')::TIMESTAMPTZ AS started_at,
    (stage.value ->> 'finished_at')::TIMESTAMPTZ AS finished_at,
    stage.value ->> 'failure_code' AS failure_code,
    run.recorded_at
FROM risk_platform.local_schedule_runs run
CROSS JOIN LATERAL jsonb_array_elements(run.run_json -> 'sessions')
    WITH ORDINALITY AS session(value, ordinality)
CROSS JOIN LATERAL jsonb_array_elements(session.value -> 'stages')
    WITH ORDINALITY AS stage(value, ordinality);

CREATE OR REPLACE VIEW risk_platform.recent_local_schedule_runs AS
SELECT
    run.*,
    ROW_NUMBER() OVER (
        PARTITION BY run.schedule_id
        ORDER BY run.started_at DESC, run.run_id DESC
    ) AS schedule_recency_rank
FROM risk_platform.local_schedule_runs run;

CREATE OR REPLACE VIEW risk_platform.current_local_schedule_run_status AS
SELECT *
FROM risk_platform.recent_local_schedule_runs
WHERE schedule_recency_rank = 1;

CREATE OR REPLACE VIEW risk_platform.current_local_schedule_run_failures AS
SELECT *
FROM risk_platform.current_local_schedule_run_status
WHERE run_status = 'failed';

CREATE OR REPLACE VIEW risk_platform.incomplete_local_schedule_sessions AS
SELECT *
FROM risk_platform.local_schedule_run_session_history
WHERE session_status IN ('selected', 'failed');
