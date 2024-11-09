test_name: test_metric_time_quarter_alone
test_filename: test_metric_time_without_metrics.py
sql_engine: BigQuery
---
-- Time Spine
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__quarter',]
SELECT
  DATETIME_TRUNC(ds, quarter) AS metric_time__quarter
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  metric_time__quarter
