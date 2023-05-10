-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  ds
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'ds']
  SELECT
    ds
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_1
) subq_2
GROUP BY
  ds
