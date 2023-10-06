-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By ['ds__day', 'bookings']
SELECT
  ds__day
  , is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'is_instant', 'ds__day']
  SELECT
    DATE_TRUNC(ds, day) AS ds__day
    , is_instant
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_5
GROUP BY
  ds__day
  , is_instant
ORDER BY ds__day, bookings DESC
