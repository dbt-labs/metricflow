test_name: test_metric_time_quarter_alone
test_filename: test_metric_time_without_metrics.py
sql_engine: Trino
---
-- Read From Time Spine 'mf_time_spine'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__quarter']
-- Pass Only Elements: ['metric_time__quarter']
-- Write to DataTable
SELECT
  DATE_TRUNC('quarter', ds) AS metric_time__quarter
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  DATE_TRUNC('quarter', ds)
