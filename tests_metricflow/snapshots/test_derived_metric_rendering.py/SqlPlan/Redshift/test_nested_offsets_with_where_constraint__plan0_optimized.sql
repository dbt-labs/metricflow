test_name: test_nested_offsets_with_where_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Constrain Output with WHERE
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , nr_subq_22.bookings_offset_once AS bookings_offset_once
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          time_spine_src_28006.ds AS metric_time__day
          , SUM(nr_subq_14.bookings) AS bookings
        FROM ***************************.mf_time_spine time_spine_src_28006
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) nr_subq_14
        ON
          DATEADD(day, -5, time_spine_src_28006.ds) = nr_subq_14.metric_time__day
        GROUP BY
          time_spine_src_28006.ds
      ) nr_subq_21
    ) nr_subq_22
    ON
      DATEADD(day, -2, time_spine_src_28006.ds) = nr_subq_22.metric_time__day
  ) nr_subq_26
  WHERE metric_time__day = '2020-01-12' or metric_time__day = '2020-01-13'
) nr_subq_27
