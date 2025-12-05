test_name: test_metric_time_only
test_filename: test_metric_time_without_metrics.py
docstring:
  Tests querying only metric time.
sql_engine: Trino
---
-- Read From Time Spine 'mf_time_spine'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__day']
-- Pass Only Elements: ['metric_time__day']
-- Write to DataTable
SELECT
  ds AS metric_time__day
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  ds
