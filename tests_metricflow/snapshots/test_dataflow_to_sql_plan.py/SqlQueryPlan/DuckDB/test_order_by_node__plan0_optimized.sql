-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By ['ds__day', 'bookings']
SELECT
  ds__day
  , is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'is_instant', 'ds__day']
  SELECT
    1 AS bookings
    , is_instant
    , DATE_TRUNC('day', ds) AS ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_3
GROUP BY
  ds__day
  , is_instant
ORDER BY ds__day, bookings DESC
