-- Combine Metrics
SELECT
  subq_18.bookings AS bookings
  , subq_19.booking_payments AS booking_payments
  , COALESCE(subq_18._ts, subq_19._ts) AS _ts
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(bookings) AS bookings
    , _ts
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Plot by Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', '_ts']
    SELECT
      1 AS bookings
      , ds AS _ts
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_12
  GROUP BY
    _ts
) subq_18
FULL OUTER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Plot by Time Dimension 'booking_paid_at'
  -- Pass Only Elements:
  --   ['booking_payments', '_ts']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(booking_value) AS booking_payments
    , booking_paid_at AS _ts
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
  GROUP BY
    booking_paid_at
) subq_19
ON
  subq_18._ts = subq_19._ts
