test_name: test_nested_derived_metric_offset_with_joined_where_constraint_not_selected
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
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
      , nr_subq_23.booking__is_instant AS booking__is_instant
      , nr_subq_23.bookings_offset_once AS bookings_offset_once
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
          , nr_subq_15.booking__is_instant AS booking__is_instant
          , SUM(nr_subq_15.bookings) AS bookings
        FROM ***************************.mf_time_spine time_spine_src_28006
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) nr_subq_15
        ON
          DATEADD(day, -5, time_spine_src_28006.ds) = nr_subq_15.metric_time__day
        GROUP BY
          time_spine_src_28006.ds
          , nr_subq_15.booking__is_instant
      ) nr_subq_22
    ) nr_subq_23
    ON
      DATEADD(day, -2, time_spine_src_28006.ds) = nr_subq_23.metric_time__day
  ) nr_subq_27
  WHERE booking__is_instant
) nr_subq_29
