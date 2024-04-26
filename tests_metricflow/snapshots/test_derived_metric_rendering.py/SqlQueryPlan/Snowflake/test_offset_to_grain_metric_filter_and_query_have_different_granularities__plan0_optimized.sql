-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , bookings_start_of_month AS bookings_at_start_of_month
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , SUM(bookings) AS bookings_start_of_month
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__month', 'metric_time__day']
    SELECT
      subq_13.ds AS metric_time__day
      , DATE_TRUNC('month', subq_13.ds) AS metric_time__month
      , subq_11.bookings AS bookings
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    ON
      DATE_TRUNC('month', subq_13.ds) = subq_11.metric_time__day
    WHERE DATE_TRUNC('month', subq_13.ds) = subq_13.ds
  ) subq_15
  WHERE metric_time__day = '2020-01-01'
  GROUP BY
    metric_time__month
) subq_19
