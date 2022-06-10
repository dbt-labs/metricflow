-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By ['ds', 'bookings']
SELECT
  SUM(bookings) AS bookings
  , is_instant
  , ds
FROM (
  -- Read Elements From Data Source 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'is_instant', 'ds']
  SELECT
    1 AS bookings
    , is_instant
    , ds
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
) subq_5
GROUP BY
  is_instant
  , ds
ORDER BY ds, bookings DESC
