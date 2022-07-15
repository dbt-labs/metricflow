-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By [] Limit 1
SELECT
  SUM(bookings) AS bookings
  , ds
FROM (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'ds']
  SELECT
    1 AS bookings
    , ds
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
) subq_7
GROUP BY
  ds
LIMIT 1
