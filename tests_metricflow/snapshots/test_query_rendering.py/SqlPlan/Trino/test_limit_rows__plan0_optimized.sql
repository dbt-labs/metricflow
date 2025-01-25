test_name: test_limit_rows
test_filename: test_query_rendering.py
docstring:
  Tests a plan with a limit to the number of rows returned.
sql_engine: Trino
---
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By [] Limit 1
SELECT
  ds__day
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'ds__day']
  SELECT
    DATE_TRUNC('day', ds) AS ds__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) nr_subq_5
GROUP BY
  ds__day
LIMIT 1
