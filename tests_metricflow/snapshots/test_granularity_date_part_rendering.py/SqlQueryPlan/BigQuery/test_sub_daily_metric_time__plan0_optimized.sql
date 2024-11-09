test_name: test_sub_daily_metric_time
test_filename: test_granularity_date_part_rendering.py
---
-- Time Spine
-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__millisecond',]
SELECT
  DATETIME_TRUNC(ts, millisecond) AS metric_time__millisecond
FROM ***************************.mf_time_spine_millisecond time_spine_src_28002
GROUP BY
  metric_time__millisecond
