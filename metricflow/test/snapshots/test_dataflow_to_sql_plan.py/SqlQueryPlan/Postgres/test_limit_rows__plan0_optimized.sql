-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By [] Limit 1
SELECT
  ds__day
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'ds__day']
  SELECT
    DATE_TRUNC('day', ds) AS ds__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_7
GROUP BY
  ds__day
LIMIT 1
