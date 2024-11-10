test_name: test_min_max_metric_time
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get the min & max distinct values of metric_time.
sql_engine: BigQuery
---
-- Calculate min and max
SELECT
  MIN(metric_time__day) AS metric_time__day__min
  , MAX(metric_time__day) AS metric_time__day__max
FROM (
  -- Time Spine
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['metric_time__day',]
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
  FROM ***************************.mf_time_spine time_spine_src_28006
  GROUP BY
    metric_time__day
) subq_5
