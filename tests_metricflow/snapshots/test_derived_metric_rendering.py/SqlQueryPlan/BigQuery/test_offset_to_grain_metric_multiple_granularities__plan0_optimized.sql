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
    , DATE_TRUNC(subq_11.ds, month) AS metric_time__month
    , DATE_TRUNC(subq_11.ds, year) AS metric_time__year
    , SUM(subq_9.bookings) AS bookings_start_of_month
  FROM ***************************.mf_time_spine subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  ON
    DATE_TRUNC(subq_11.ds, month) = subq_9.metric_time__day
  GROUP BY
    metric_time__day
    , metric_time__month
    , metric_time__year
) subq_15
