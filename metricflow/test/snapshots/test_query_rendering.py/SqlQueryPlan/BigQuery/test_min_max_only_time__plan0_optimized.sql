-- Calculate min and max
SELECT
  MIN(booking__paid_at__day) AS booking__paid_at__day__min
  , MAX(booking__paid_at__day) AS booking__paid_at__day__max
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['booking__paid_at__day',]
  SELECT
    DATE_TRUNC(paid_at, day) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_10001
  GROUP BY
    booking__paid_at__day
) subq_3
