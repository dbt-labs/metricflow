-- Calculate min and max
SELECT
  MIN(subq_0.booking__paid_at__day) AS booking__paid_at__day__min
  , MAX(subq_0.booking__paid_at__day) AS booking__paid_at__day__max
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['booking__paid_at__day',]
  SELECT
    DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', bookings_source_src_28000.paid_at)
) subq_0
