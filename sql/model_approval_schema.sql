-- Append-only model approval and revocation evidence.
-- Apply after the method-aware attribution and risk-limit schemas.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.model_approvals (
    approval_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (model_version = 'model-approval-v1'),
    use_case TEXT NOT NULL,
    contract_fingerprint TEXT NOT NULL,
    attribution_model_version TEXT NOT NULL,
    weighting_method TEXT NOT NULL,
    covariance_method TEXT NOT NULL,
    correlation_method TEXT NOT NULL,
    fixed_parameters_json JSONB NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    approved_at TIMESTAMPTZ NOT NULL,
    approved_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (approval_id ~ '^model-approval-v1-[0-9a-f]{24}$'),
    CHECK (use_case ~ '^[a-z0-9][a-z0-9._:-]{0,127}$'),
    CHECK (contract_fingerprint ~ '^model-contract-v1-[0-9a-f]{24}$'),
    CHECK (request_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        approved_by = BTRIM(approved_by)
        AND CHAR_LENGTH(approved_by) BETWEEN 1 AND 128
        AND approved_by !~ '[[:cntrl:]]'
    ),
    CHECK (
        reason = BTRIM(reason)
        AND CHAR_LENGTH(reason) BETWEEN 1 AND 2000
        AND reason !~ '[[:cntrl:]]'
    ),
    CHECK (jsonb_typeof(fixed_parameters_json) = 'object'),
    CHECK (
        (
            attribution_model_version = 'portfolio-attribution-v1'
            AND weighting_method = 'constant_weight_daily_rebalanced'
            AND covariance_method = 'sample_annualized'
            AND correlation_method = 'pearson'
            AND fixed_parameters_json = '{"annualization_days":252,"degrees_of_freedom":1,"estimator":"sample"}'::JSONB
        )
        OR (
            attribution_model_version = 'portfolio-attribution-ewma-v1'
            AND weighting_method = 'constant_weight_daily_rebalanced'
            AND covariance_method =
                'ewma_zero_mean_lambda_0_94_annualized'
            AND correlation_method =
                'implied_from_ewma_covariance'
            AND fixed_parameters_json = '{"annualization_days":252,"decay":0.94,"mean_assumption":"zero_daily"}'::JSONB
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_model_approvals_current_lookup
    ON risk_platform.model_approvals (
        use_case,
        contract_fingerprint,
        approved_at DESC,
        approval_id DESC
    );

CREATE TABLE IF NOT EXISTS risk_platform.model_approval_revocations (
    revocation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'model-approval-revocation-v1'
    ),
    approval_id TEXT NOT NULL REFERENCES
        risk_platform.model_approvals (approval_id),
    request_id TEXT NOT NULL UNIQUE,
    revoked_at TIMESTAMPTZ NOT NULL,
    revoked_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        revocation_id ~ '^model-approval-revocation-v1-[0-9a-f]{24}$'
    ),
    CHECK (request_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (
        revoked_by = BTRIM(revoked_by)
        AND CHAR_LENGTH(revoked_by) BETWEEN 1 AND 128
        AND revoked_by !~ '[[:cntrl:]]'
    ),
    CHECK (
        reason = BTRIM(reason)
        AND CHAR_LENGTH(reason) BETWEEN 1 AND 2000
        AND reason !~ '[[:cntrl:]]'
    )
);

CREATE INDEX IF NOT EXISTS idx_model_approval_revocations_target
    ON risk_platform.model_approval_revocations (
        approval_id,
        revoked_at DESC,
        revocation_id DESC
    );

CREATE OR REPLACE FUNCTION risk_platform.validate_model_approval_revocation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_approved_at TIMESTAMPTZ;
BEGIN
    SELECT approval.approved_at
    INTO target_approved_at
    FROM risk_platform.model_approvals approval
    WHERE approval.approval_id = NEW.approval_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'model approval revocation target does not exist';
    END IF;
    IF NEW.revoked_at < target_approved_at THEN
        RAISE EXCEPTION
            'revoked_at must be on or after the approval timestamp';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION risk_platform.prevent_model_governance_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'model governance evidence is append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS validate_model_approval_revocation_insert
    ON risk_platform.model_approval_revocations;
CREATE TRIGGER validate_model_approval_revocation_insert
BEFORE INSERT ON risk_platform.model_approval_revocations
FOR EACH ROW
EXECUTE FUNCTION risk_platform.validate_model_approval_revocation();

DROP TRIGGER IF EXISTS prevent_model_approval_update
    ON risk_platform.model_approvals;
CREATE TRIGGER prevent_model_approval_update
BEFORE UPDATE ON risk_platform.model_approvals
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_model_governance_mutation();

DROP TRIGGER IF EXISTS prevent_model_approval_delete
    ON risk_platform.model_approvals;
CREATE TRIGGER prevent_model_approval_delete
BEFORE DELETE ON risk_platform.model_approvals
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_model_governance_mutation();

DROP TRIGGER IF EXISTS prevent_model_approval_revocation_update
    ON risk_platform.model_approval_revocations;
CREATE TRIGGER prevent_model_approval_revocation_update
BEFORE UPDATE ON risk_platform.model_approval_revocations
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_model_governance_mutation();

DROP TRIGGER IF EXISTS prevent_model_approval_revocation_delete
    ON risk_platform.model_approval_revocations;
CREATE TRIGGER prevent_model_approval_revocation_delete
BEFORE DELETE ON risk_platform.model_approval_revocations
FOR EACH ROW
EXECUTE FUNCTION risk_platform.prevent_model_governance_mutation();

CREATE OR REPLACE VIEW risk_platform.model_approval_event_history AS
SELECT
    approval.approval_id AS event_id,
    'approval'::TEXT AS event_type,
    approval.model_version,
    approval.approval_id,
    approval.request_id,
    approval.approved_at AS event_at,
    approval.approved_by AS actor,
    approval.reason,
    approval.use_case,
    approval.contract_fingerprint,
    approval.attribution_model_version,
    approval.weighting_method,
    approval.covariance_method,
    approval.correlation_method,
    approval.fixed_parameters_json
FROM risk_platform.model_approvals approval

UNION ALL

SELECT
    revocation.revocation_id AS event_id,
    'revocation'::TEXT AS event_type,
    revocation.model_version,
    revocation.approval_id,
    revocation.request_id,
    revocation.revoked_at AS event_at,
    revocation.revoked_by AS actor,
    revocation.reason,
    approval.use_case,
    approval.contract_fingerprint,
    approval.attribution_model_version,
    approval.weighting_method,
    approval.covariance_method,
    approval.correlation_method,
    approval.fixed_parameters_json
FROM risk_platform.model_approval_revocations revocation
JOIN risk_platform.model_approvals approval
    ON approval.approval_id = revocation.approval_id;

CREATE OR REPLACE VIEW risk_platform.current_model_approval_status AS
WITH ranked_approvals AS (
    SELECT
        approval.*,
        COUNT(*) OVER (
            PARTITION BY approval.use_case, approval.contract_fingerprint
        ) AS approval_count,
        ROW_NUMBER() OVER (
            PARTITION BY approval.use_case, approval.contract_fingerprint
            ORDER BY approval.approved_at DESC, approval.approval_id DESC
        ) AS approval_rank
    FROM risk_platform.model_approvals approval
)
SELECT
    approval.approval_id,
    approval.model_version,
    approval.use_case,
    approval.contract_fingerprint,
    approval.attribution_model_version,
    approval.weighting_method,
    approval.covariance_method,
    approval.correlation_method,
    approval.fixed_parameters_json,
    approval.request_id AS approval_request_id,
    approval.approved_at,
    approval.approved_by,
    approval.reason AS approval_reason,
    approval.approval_count,
    latest_revocation.revocation_id,
    latest_revocation.request_id AS revocation_request_id,
    latest_revocation.revoked_at,
    latest_revocation.revoked_by,
    latest_revocation.reason AS revocation_reason,
    COALESCE(latest_revocation.revocation_count, 0) AS revocation_count,
    CASE
        WHEN latest_revocation.revocation_id IS NULL THEN 'approved'
        ELSE 'revoked'
    END AS approval_status
FROM ranked_approvals approval
LEFT JOIN LATERAL (
    SELECT
        revocation.revocation_id,
        revocation.request_id,
        revocation.revoked_at,
        revocation.revoked_by,
        revocation.reason,
        COUNT(*) OVER () AS revocation_count
    FROM risk_platform.model_approval_revocations revocation
    WHERE revocation.approval_id = approval.approval_id
    ORDER BY revocation.revoked_at DESC, revocation.revocation_id DESC
    LIMIT 1
) latest_revocation ON TRUE
WHERE approval.approval_rank = 1;

CREATE OR REPLACE VIEW risk_platform.current_model_approvals AS
SELECT *
FROM risk_platform.current_model_approval_status
WHERE approval_status = 'approved';

CREATE OR REPLACE VIEW risk_platform.revoked_model_approvals AS
SELECT *
FROM risk_platform.current_model_approval_status
WHERE approval_status = 'revoked';
