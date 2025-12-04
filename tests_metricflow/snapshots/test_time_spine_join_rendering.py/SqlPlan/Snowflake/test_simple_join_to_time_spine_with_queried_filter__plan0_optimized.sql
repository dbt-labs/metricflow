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
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Constrain Output with WHERE
  SELECT
    bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
    , booking__is_instant
    , metric_time__day
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_18.booking__is_instant AS booking__is_instant
      , subq_18.__bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'booking__is_instant', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , booking__is_instant
        , SUM(bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'booking__is_instant', 'metric_time__day']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , is_instant AS booking__is_instant
          , 1 AS bookings_fill_nulls_with_0
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_15
      WHERE booking__is_instant
      GROUP BY
        metric_time__day
        , booking__is_instant
    ) subq_18
    ON
      time_spine_src_28006.ds = subq_18.metric_time__day
  ) subq_23
  WHERE booking__is_instant
) subq_24
