-- Example data quality checks
-- Required-field validation status emitted by the pipeline.
SELECT
  ts_ingest,
  required_fields_status,
  required_fields_checked,
  missing_required_field_count,
  missing_required_record_count,
  missing_required_fields_by_name,
  null_rate_status,
  null_fields_checked,
  null_field_count,
  null_record_count,
  max_null_rate,
  null_fields_by_name,
  null_rates_by_name,
  value_validity_status,
  value_fields_checked,
  invalid_value_count,
  invalid_value_record_count,
  invalid_values_by_name
FROM data_quality_metrics
WHERE required_fields_status <> 'ok'
  OR null_rate_status <> 'ok'
  OR value_validity_status <> 'ok';
