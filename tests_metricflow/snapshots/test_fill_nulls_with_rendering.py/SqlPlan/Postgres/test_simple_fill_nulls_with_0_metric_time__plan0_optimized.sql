test_name: test_simple_fill_nulls_with_0_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COALESCE(bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_12.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings_fill_nulls_with_0) AS bookings_fill_nulls_with_0
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings_fill_nulls_with_0', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    GROUP BY
      metric_time__day
  ) subq_12
  ON
    time_spine_src_28006.ds = subq_12.metric_time__day
) subq_16
