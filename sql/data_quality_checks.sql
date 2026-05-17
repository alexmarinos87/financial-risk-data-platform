-- Example data quality checks
-- Required-field validation status emitted by the pipeline.
SELECT
  ts_ingest,
  required_fields_status,
  required_fields_checked,
  missing_required_field_count,
  missing_required_record_count,
  missing_required_fields_by_name
FROM data_quality_metrics
WHERE required_fields_status <> 'ok';
