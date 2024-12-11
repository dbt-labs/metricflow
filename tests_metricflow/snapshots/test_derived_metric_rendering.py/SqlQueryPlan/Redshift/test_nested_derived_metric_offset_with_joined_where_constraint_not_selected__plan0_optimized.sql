test_name: test_nested_derived_metric_offset_with_joined_where_constraint_not_selected
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day', 'bookings_offset_once']
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_25.booking__is_instant AS booking__is_instant
      , subq_25.bookings_offset_once AS bookings_offset_once
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking__is_instant
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          time_spine_src_28006.ds AS metric_time__day
          , subq_17.booking__is_instant AS booking__is_instant
          , SUM(subq_17.bookings) AS bookings
        FROM ***************************.mf_time_spine time_spine_src_28006
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_17
        ON
          DATEADD(day, -5, time_spine_src_28006.ds) = subq_17.metric_time__day
        GROUP BY
          time_spine_src_28006.ds
          , subq_17.booking__is_instant
      ) subq_24
    ) subq_25
    ON
      DATEADD(day, -2, time_spine_src_28006.ds) = subq_25.metric_time__day
  ) subq_29
  WHERE booking__is_instant
) subq_31
