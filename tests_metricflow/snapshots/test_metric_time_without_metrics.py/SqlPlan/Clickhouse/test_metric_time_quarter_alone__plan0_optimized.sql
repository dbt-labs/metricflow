test_name: test_metric_time_quarter_alone
test_filename: test_metric_time_without_metrics.py
sql_engine: Clickhouse
---
-- Read From Time Spine 'mf_time_spine'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__quarter',]
SELECT
  DATE_TRUNC('quarter', ds) AS metric_time__quarter
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  DATE_TRUNC('quarter', ds)
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
