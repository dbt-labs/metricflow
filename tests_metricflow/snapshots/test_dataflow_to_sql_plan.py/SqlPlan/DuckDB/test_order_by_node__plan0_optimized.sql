test_name: test_order_by_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node.
sql_engine: DuckDB
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Order By ['ds__day', 'bookings']
SELECT
  ds__day
  , is_instant
  , SUM(__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['__bookings', 'is_instant', 'ds__day']
  SELECT
    DATE_TRUNC('day', ds) AS ds__day
    , is_instant
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_5
GROUP BY
  ds__day
  , is_instant
ORDER BY ds__day, bookings DESC
