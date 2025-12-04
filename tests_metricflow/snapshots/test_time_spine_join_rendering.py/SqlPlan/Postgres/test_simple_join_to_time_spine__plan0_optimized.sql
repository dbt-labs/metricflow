test_name: test_simple_join_to_time_spine
test_filename: test_time_spine_join_rendering.py
sql_engine: Postgres
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  time_spine_src_28006.ds AS metric_time__day
  , subq_15.__bookings_join_to_time_spine AS bookings_join_to_time_spine
FROM ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN (
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__day
    , SUM(__bookings_join_to_time_spine) AS __bookings_join_to_time_spine
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__bookings_join_to_time_spine', 'metric_time__day']
    -- Pass Only Elements: ['__bookings_join_to_time_spine', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS __bookings_join_to_time_spine
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_14
  GROUP BY
    metric_time__day
) subq_15
ON
  time_spine_src_28006.ds = subq_15.metric_time__day
