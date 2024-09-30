-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_11.metric_time__day, subq_15.metric_time__day) AS metric_time__day
  , MAX(subq_11.bookings) AS bookings
  , MAX(subq_15.booking_payments) AS booking_payments
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  GROUP BY
    metric_time__day
) subq_11
FULL OUTER JOIN (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'paid_at'
  -- Pass Only Elements: ['booking_payments', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', paid_at) AS metric_time__day
    , SUM(booking_value) AS booking_payments
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', paid_at)
) subq_15
ON
  subq_11.metric_time__day = subq_15.metric_time__day
GROUP BY
  COALESCE(subq_11.metric_time__day, subq_15.metric_time__day)
