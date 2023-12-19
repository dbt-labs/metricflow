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
      subq_23.ds AS metric_time__day
      , subq_21.bookings_offset_once AS bookings_offset_once
    FROM ***************************.mf_time_spine subq_23
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements:
        --   ['bookings', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          subq_16.ds AS metric_time__day
          , SUM(subq_14.bookings) AS bookings
        FROM ***************************.mf_time_spine subq_16
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_10001
        ) subq_14
        ON
          DATE_ADD('day', -5, subq_16.ds) = subq_14.metric_time__day
        GROUP BY
          subq_16.ds
      ) subq_20
    ) subq_21
    ON
      DATE_ADD('day', -2, subq_23.ds) = subq_21.metric_time__day
  ) subq_24
  WHERE metric_time__day = '2020-01-12' or metric_time__day = '2020-01-13'
) subq_25
