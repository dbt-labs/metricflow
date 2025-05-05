test_name: test_no_metric_custom_granularity_metric_time
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__alien_day']
-- Write to DataTable
SELECT
  subq_5.alien_day AS metric_time__alien_day
FROM ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_5
ON
  time_spine_src_28006.ds = subq_5.ds
GROUP BY
  subq_5.alien_day
