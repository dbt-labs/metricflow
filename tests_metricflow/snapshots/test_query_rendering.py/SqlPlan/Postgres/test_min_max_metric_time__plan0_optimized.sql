test_name: test_min_max_metric_time
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get the min & max distinct values of metric_time.
sql_engine: Postgres
---
-- Calculate min and max
-- Write to DataTable
SELECT
  MIN(metric_time__day) AS metric_time__day__min
  , MAX(metric_time__day) AS metric_time__day__max
FROM (
  -- Read From Time Spine 'mf_time_spine'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['metric_time__day']
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine time_spine_src_28006
  GROUP BY
    ds
) subq_8
