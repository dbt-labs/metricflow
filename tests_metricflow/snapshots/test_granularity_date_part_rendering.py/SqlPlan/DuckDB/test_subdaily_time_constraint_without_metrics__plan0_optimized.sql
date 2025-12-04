test_name: test_subdaily_time_constraint_without_metrics
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Read From Time Spine 'mf_time_spine_second'
-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__second']
-- Constrain Time Range to [2020-01-01T00:00:02, 2020-01-01T00:00:08]
-- Pass Only Elements: ['metric_time__second']
-- Write to DataTable
SELECT
  ts AS metric_time__second
FROM ***************************.mf_time_spine_second time_spine_src_28003
WHERE ts BETWEEN '2020-01-01 00:00:02' AND '2020-01-01 00:00:08'
GROUP BY
  ts
