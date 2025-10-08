test_name: test_simple_join_to_time_spine_with_filter
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric fills nulls and filter is not in group by. Should apply constraint once.
sql_engine: Databricks
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
    , subq_14.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings_fill_nulls_with_0', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings_fill_nulls_with_0) AS bookings_fill_nulls_with_0
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , is_instant AS booking__is_instant
        , 1 AS bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_14
  ON
    time_spine_src_28006.ds = subq_14.metric_time__day
) subq_18
