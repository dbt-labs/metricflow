test_name: test_nested_offsets
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , nr_subq_21.bookings_offset_once AS bookings_offset_once
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
        , SUM(nr_subq_13.bookings) AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) nr_subq_13
      ON
        DATEADD(day, -5, time_spine_src_28006.ds) = nr_subq_13.metric_time__day
      GROUP BY
        time_spine_src_28006.ds
    ) nr_subq_20
  ) nr_subq_21
  ON
    DATEADD(day, -2, time_spine_src_28006.ds) = nr_subq_21.metric_time__day
) nr_subq_25
