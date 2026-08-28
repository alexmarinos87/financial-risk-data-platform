-- Reconciliation for current activation and controlled receiver review views.

WITH counts AS (
    SELECT
        (SELECT COUNT(DISTINCT destination_id)
         FROM risk_platform.notification_activation_checklists)
            AS destination_count,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_activation_checklists)
            AS latest_checklist_rows,
        (SELECT COUNT(DISTINCT destination_id)
         FROM risk_platform.controlled_notification_receiver_rehearsals)
            AS rehearsal_destination_count,
        (SELECT COUNT(*)
         FROM risk_platform.latest_controlled_notification_receiver_rehearsals)
            AS latest_rehearsal_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_activation_rehearsal_review)
            AS review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_activation_rehearsal_review
         WHERE review_required)
            AS expected_failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_activation_review_failures)
            AS failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_activation_rehearsal_review
         WHERE operational_activation_ready)
            AS expected_ready_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_activation_ready)
            AS ready_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id
                FROM risk_platform.latest_notification_activation_checklists
                GROUP BY destination_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_checklists,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_notification_activation_checklists latest
            WHERE latest.current_version_rank <> 1
               OR EXISTS (
                   SELECT 1
                   FROM risk_platform.notification_activation_checklists candidate
                   WHERE candidate.destination_id = latest.destination_id
                     AND (candidate.reviewed_at, candidate.checklist_id)
                         > (latest.reviewed_at, latest.checklist_id)
               )
        ) AS stale_latest_checklists,
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id
                FROM risk_platform.latest_controlled_notification_receiver_rehearsals
                GROUP BY destination_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_rehearsals,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_controlled_notification_receiver_rehearsals
                latest
            WHERE latest.current_version_rank <> 1
               OR EXISTS (
                   SELECT 1
                   FROM risk_platform.controlled_notification_receiver_rehearsals
                       candidate
                   WHERE candidate.destination_id = latest.destination_id
                     AND (candidate.recorded_at, candidate.record_id)
                         > (latest.recorded_at, latest.record_id)
               )
        ) AS stale_latest_rehearsals,
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id
                FROM risk_platform.current_notification_activation_rehearsal_review
                GROUP BY destination_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_review_destinations,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_activation_rehearsal_review
                review
            WHERE jsonb_array_length(review.incomplete_controls_json) <> (
                SELECT COUNT(*)
                FROM jsonb_each(review.controls_json) control
                WHERE control.value <> 'true'::jsonb
            )
        ) AS incomplete_control_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_activation_rehearsal_review
                review
            WHERE review.review_status <> CASE
                WHEN NOT review.checklist_activation_ready
                    THEN 'checklist_incomplete'
                WHEN CURRENT_TIMESTAMP < review.reviewed_at
                    THEN 'checklist_not_yet_active'
                WHEN CURRENT_TIMESTAMP >= review.review_expires_at
                    THEN 'checklist_expired'
                WHEN review.rehearsal_record_id IS NULL
                    THEN 'rehearsal_missing'
                WHEN NOT review.rehearsal_reference_consistent
                    THEN 'rehearsal_evidence_conflict'
                WHEN NOT review.rehearsal_matches_current_checklist
                    THEN 'rehearsal_superseded'
                WHEN review.rehearsal_terminal_status
                    = 'rejected_before_request'
                    THEN 'rehearsal_rejected'
                WHEN review.rehearsal_terminal_status
                    = 'failed_during_rehearsal'
                    THEN 'rehearsal_failed'
                WHEN review.rehearsal_terminal_status = 'completed'
                    THEN 'ready'
                ELSE 'review_required'
            END
        ) AS review_status_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_activation_rehearsal_review
                review
            WHERE review.review_required = (review.review_status = 'ready')
               OR review.operational_activation_ready
                    <> (review.review_status = 'ready')
        ) AS review_flag_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_activation_ready ready
            WHERE ready.review_status <> 'ready'
               OR ready.review_required
               OR NOT ready.operational_activation_ready
               OR NOT ready.checklist_activation_ready
               OR ready.rehearsal_terminal_status <> 'completed'
               OR NOT ready.rehearsal_reference_consistent
               OR NOT ready.rehearsal_matches_current_checklist
               OR CURRENT_TIMESTAMP < ready.reviewed_at
               OR CURRENT_TIMESTAMP >= ready.review_expires_at
        ) AS invalid_ready_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_activation_review_failures
                failure
            WHERE NOT failure.review_required
               OR failure.operational_activation_ready
               OR failure.review_status = 'ready'
        ) AS invalid_failure_rows
)
SELECT
    'notification_activation_latest_checklist_covers_destinations' AS check_name,
    destination_count::TEXT AS expected,
    latest_checklist_rows::TEXT AS actual,
    CASE
        WHEN destination_count = latest_checklist_rows THEN 'pass'
        ELSE 'fail'
    END AS status
FROM counts

UNION ALL

SELECT
    'notification_activation_latest_checklist_grain_unique',
    '0',
    duplicate_latest_checklists::TEXT,
    CASE WHEN duplicate_latest_checklists = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_latest_checklist_selection_current',
    '0',
    stale_latest_checklists::TEXT,
    CASE WHEN stale_latest_checklists = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_latest_rehearsal_covers_destinations',
    rehearsal_destination_count::TEXT,
    latest_rehearsal_rows::TEXT,
    CASE
        WHEN rehearsal_destination_count = latest_rehearsal_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'controlled_receiver_latest_rehearsal_grain_unique',
    '0',
    duplicate_latest_rehearsals::TEXT,
    CASE WHEN duplicate_latest_rehearsals = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_latest_rehearsal_selection_current',
    '0',
    stale_latest_rehearsals::TEXT,
    CASE WHEN stale_latest_rehearsals = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_review_covers_destinations',
    destination_count::TEXT,
    review_rows::TEXT,
    CASE WHEN destination_count = review_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_activation_review_grain_unique',
    '0',
    duplicate_review_destinations::TEXT,
    CASE WHEN duplicate_review_destinations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_incomplete_controls_reconcile',
    '0',
    incomplete_control_mismatches::TEXT,
    CASE WHEN incomplete_control_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_review_status_reconciles',
    '0',
    review_status_mismatches::TEXT,
    CASE WHEN review_status_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_review_flags_reconcile',
    '0',
    review_flag_mismatches::TEXT,
    CASE WHEN review_flag_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_failure_partition_matches',
    expected_failure_rows::TEXT,
    failure_rows::TEXT,
    CASE WHEN expected_failure_rows = failure_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_activation_failure_rows_valid',
    '0',
    invalid_failure_rows::TEXT,
    CASE WHEN invalid_failure_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_ready_partition_matches',
    expected_ready_rows::TEXT,
    ready_rows::TEXT,
    CASE WHEN expected_ready_rows = ready_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_activation_ready_rows_valid',
    '0',
    invalid_ready_rows::TEXT,
    CASE WHEN invalid_ready_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
