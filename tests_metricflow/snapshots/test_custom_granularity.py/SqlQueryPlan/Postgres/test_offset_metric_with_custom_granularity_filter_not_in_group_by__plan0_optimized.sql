-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings_5_days_ago
  FROM (
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    SELECT
      subq_15.metric_time__day AS metric_time__day
      , subq_15.bookings AS bookings
      , subq_16.martian_day AS metric_time__martian_day
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_14.ds AS metric_time__day
        , subq_12.bookings AS bookings
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
        subq_14.ds - MAKE_INTERVAL(days => 5) = subq_12.metric_time__day
    ) subq_15
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_16
    ON
      subq_15.metric_time__day = subq_16.ds
  ) subq_17
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__day
) subq_21