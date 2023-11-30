-- Calculate min and max
SELECT
  MIN(booking__paid_at__day) AS min_booking__paid_at__day
  , MAX(booking__paid_at__day) AS max_booking__paid_at__day
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['booking__paid_at__day']
  SELECT
    DATE_TRUNC('day', paid_at) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_10001
  GROUP BY
    DATE_TRUNC('day', paid_at)
) subq_3
