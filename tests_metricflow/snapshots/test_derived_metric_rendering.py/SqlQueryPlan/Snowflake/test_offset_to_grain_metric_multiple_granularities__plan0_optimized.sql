-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , metric_time__month
  , metric_time__year
  , bookings_start_of_month AS bookings_at_start_of_month
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__month', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_11.ds AS metric_time__day
    , DATE_TRUNC('month', subq_11.ds) AS metric_time__month
    , DATE_TRUNC('year', subq_11.ds) AS metric_time__year
    , SUM(subq_9.bookings) AS bookings_start_of_month
  FROM ***************************.mf_time_spine subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  ON
    DATE_TRUNC('month', subq_11.ds) = subq_9.metric_time__day
  GROUP BY
    subq_11.ds
    , DATE_TRUNC('month', subq_11.ds)
    , DATE_TRUNC('year', subq_11.ds)
) subq_15
