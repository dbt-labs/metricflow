test_name: test_nested_derived_metric_offset_with_joined_where_constraint_not_selected
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_24.ds AS metric_time__day
    , subq_22.bookings_offset_once AS bookings_offset_once
  FROM ***************************.mf_time_spine subq_24
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_16.ds AS metric_time__day
          , subq_14.booking__is_instant AS booking__is_instant
          , subq_14.bookings AS bookings
        FROM ***************************.mf_time_spine subq_16
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_14
        ON
          subq_16.ds - INTERVAL 5 day = subq_14.metric_time__day
      ) subq_17
      WHERE booking__is_instant
      GROUP BY
        metric_time__day
    ) subq_21
  ) subq_22
  ON
    subq_24.ds - INTERVAL 2 day = subq_22.metric_time__day
) subq_25
