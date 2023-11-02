-- Compute Metrics via Expressions
SELECT
  booking__paid_at__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Aggregate Measures
  SELECT
    booking__paid_at__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'booking__paid_at__day']
    SELECT
      DATE_TRUNC('day', paid_at) AS booking__paid_at__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_6
  GROUP BY
    booking__paid_at__day
) subq_7
