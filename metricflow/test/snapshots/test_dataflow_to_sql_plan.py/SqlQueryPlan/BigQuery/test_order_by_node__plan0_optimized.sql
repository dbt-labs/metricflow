-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By ['ds', 'bookings']
SELECT
  ds
  , is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'is_instant', 'ds']
  SELECT
    ds
    , is_instant
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_5
GROUP BY
  ds
  , is_instant
ORDER BY ds, bookings DESC
