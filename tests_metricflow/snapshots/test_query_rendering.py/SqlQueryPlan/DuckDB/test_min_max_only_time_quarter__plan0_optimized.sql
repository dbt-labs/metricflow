-- Calculate min and max
SELECT
  MIN(booking__paid_at__quarter) AS booking__paid_at__quarter__min
  , MAX(booking__paid_at__quarter) AS booking__paid_at__quarter__max
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['booking__paid_at__quarter',]
  SELECT
    DATE_TRUNC('quarter', paid_at) AS booking__paid_at__quarter
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('quarter', paid_at)
) subq_1
