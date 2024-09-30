-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_20.ds AS metric_time__day
    , subq_18.bookings_offset_once AS bookings_offset_once
  FROM ***************************.mf_time_spine subq_20
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
        subq_14.ds AS metric_time__day
        , SUM(subq_12.bookings) AS bookings
      FROM ***************************.mf_time_spine subq_14
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_12
      ON
        subq_14.ds - INTERVAL 5 day = subq_12.metric_time__day
      GROUP BY
        subq_14.ds
    ) subq_17
  ) subq_18
  ON
    subq_20.ds - INTERVAL 2 day = subq_18.metric_time__day
) subq_21
