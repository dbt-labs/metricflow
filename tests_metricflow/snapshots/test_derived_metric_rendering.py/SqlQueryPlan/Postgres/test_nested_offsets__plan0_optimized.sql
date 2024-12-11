test_name: test_nested_offsets
test_filename: test_derived_metric_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_23.bookings_offset_once AS bookings_offset_once
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
        , SUM(subq_15.bookings) AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_15
      ON
        time_spine_src_28006.ds - MAKE_INTERVAL(days => 5) = subq_15.metric_time__day
      GROUP BY
        time_spine_src_28006.ds
    ) subq_22
  ) subq_23
  ON
    time_spine_src_28006.ds - MAKE_INTERVAL(days => 2) = subq_23.metric_time__day
) subq_27
