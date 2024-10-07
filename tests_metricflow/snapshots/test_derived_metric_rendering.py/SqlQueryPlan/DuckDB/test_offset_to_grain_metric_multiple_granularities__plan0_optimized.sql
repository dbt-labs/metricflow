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
    subq_10.ds AS metric_time__day
    , DATE_TRUNC('month', subq_10.ds) AS metric_time__month
    , DATE_TRUNC('year', subq_10.ds) AS metric_time__year
    , SUM(subq_8.bookings) AS bookings_start_of_month
  FROM ***************************.mf_time_spine subq_10
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_8
  ON
    DATE_TRUNC('month', subq_10.ds) = subq_8.metric_time__day
  GROUP BY
    subq_10.ds
    , DATE_TRUNC('month', subq_10.ds)
    , DATE_TRUNC('year', subq_10.ds)
) subq_13
