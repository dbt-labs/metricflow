-- Combine Metrics
SELECT
  subq_18.bookings AS bookings
  , subq_19.booking_payments AS booking_payments
  , COALESCE(subq_18.__ts, subq_19.__ts) AS __ts
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(bookings) AS bookings
    , __ts
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Plot by Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', '__ts']
    SELECT
      1 AS bookings
      , ds AS __ts
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_12
  GROUP BY
    __ts
) subq_18
FULL OUTER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Plot by Time Dimension 'booking_paid_at'
  -- Pass Only Elements:
  --   ['booking_payments', '__ts']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(booking_value) AS booking_payments
    , booking_paid_at AS __ts
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
  GROUP BY
    booking_paid_at
) subq_19
ON
  subq_18.__ts = subq_19.__ts
