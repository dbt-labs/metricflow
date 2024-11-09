test_name: test_subdaily_time_constraint_without_metrics
test_filename: test_granularity_date_part_rendering.py
---
-- Time Spine
-- Metric Time Dimension 'ts'
-- Constrain Time Range to [2020-01-01T00:00:02, 2020-01-01T00:00:08]
-- Pass Only Elements: ['metric_time__second',]
SELECT
  DATE_TRUNC('second', ts) AS metric_time__second
FROM ***************************.mf_time_spine_second time_spine_src_28003
WHERE DATE_TRUNC('second', ts) BETWEEN timestamp '2020-01-01 00:00:02' AND timestamp '2020-01-01 00:00:08'
GROUP BY
  DATE_TRUNC('second', ts)
