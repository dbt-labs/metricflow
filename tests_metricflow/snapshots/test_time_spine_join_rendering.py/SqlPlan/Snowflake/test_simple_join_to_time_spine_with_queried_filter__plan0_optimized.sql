test_name: test_simple_join_to_time_spine_with_queried_filter
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric fills nulls and filter is in group by. Should apply constraint twice.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , booking__is_instant
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Constrain Output with WHERE
  SELECT
    metric_time__day
    , booking__is_instant
    , bookings
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_15.booking__is_instant AS booking__is_instant
      , subq_15.bookings AS bookings
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        metric_time__day
        , booking__is_instant
        , SUM(bookings) AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_12
      WHERE booking__is_instant
      GROUP BY
        metric_time__day
        , booking__is_instant
    ) subq_15
    ON
      time_spine_src_28006.ds = subq_15.metric_time__day
  ) subq_19
  WHERE booking__is_instant
) subq_20
