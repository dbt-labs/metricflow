-- Combine Metrics
SELECT
  subq_18.bookings AS bookings
  , subq_19.booking_payments AS booking_payments
  , COALESCE(subq_18.metric_time, subq_19.metric_time) AS metric_time
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(bookings) AS bookings
    , metric_time
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'metric_time']
    SELECT
      1 AS bookings
      , ds AS metric_time
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_12
  GROUP BY
    metric_time
) subq_18
FULL OUTER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'booking_paid_at'
  -- Pass Only Elements:
  --   ['booking_payments', 'metric_time']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(booking_value) AS booking_payments
    , booking_paid_at AS metric_time
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
  GROUP BY
    booking_paid_at
) subq_19
ON
  subq_18.metric_time = subq_19.metric_time
