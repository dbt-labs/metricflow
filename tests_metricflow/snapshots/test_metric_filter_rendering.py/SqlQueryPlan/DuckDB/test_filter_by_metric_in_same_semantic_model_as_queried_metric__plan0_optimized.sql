-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookers', 'guest__booking_value']
  SELECT
    subq_30.guest__booking_value AS guest__booking_value
    , subq_24.bookers AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'guest']
    SELECT
      guest_id AS guest
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_24
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['booking_value', 'guest']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['guest', 'guest__booking_value']
    SELECT
      guest_id AS guest
      , SUM(booking_value) AS guest__booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      guest_id
  ) subq_30
  ON
    subq_24.guest = subq_30.guest
) subq_32
WHERE guest__booking_value > 1.00
