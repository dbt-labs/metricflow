test_name: test_sub_daily_metric_time
test_filename: test_granularity_date_part_rendering.py
sql_engine: Postgres
---
-- Read From Time Spine 'mf_time_spine_millisecond'
-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__millisecond',]
SELECT
  DATE_TRUNC('millisecond', ts) AS metric_time__millisecond
FROM ***************************.mf_time_spine_millisecond time_spine_src_28002
GROUP BY
  DATE_TRUNC('millisecond', ts)
