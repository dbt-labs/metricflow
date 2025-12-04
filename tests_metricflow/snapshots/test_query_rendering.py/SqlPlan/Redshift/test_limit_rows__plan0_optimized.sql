test_name: test_limit_rows
test_filename: test_query_rendering.py
docstring:
  Tests a plan with a limit to the number of rows returned.
sql_engine: Redshift
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Order By [] Limit 1
-- Write to DataTable
SELECT
  ds__day
  , SUM(__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__bookings', 'ds__day']
  -- Pass Only Elements: ['__bookings', 'ds__day']
  SELECT
    DATE_TRUNC('day', ds) AS ds__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_10
GROUP BY
  ds__day
LIMIT 1
