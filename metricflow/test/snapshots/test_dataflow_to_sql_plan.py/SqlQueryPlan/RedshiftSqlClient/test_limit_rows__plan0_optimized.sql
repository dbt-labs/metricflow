-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By [] Limit 1
SELECT
  ds
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'ds']
  SELECT
    ds
    , 1 AS bookings
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
) subq_7
GROUP BY
  ds
LIMIT 1
