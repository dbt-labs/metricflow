test_name: test_filter_with_where_constraint_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
SELECT
  bookings AS __bookings
  , ds__day
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['__bookings', 'ds__day']
  SELECT
    DATE_TRUNC('day', ds) AS ds__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_3
WHERE booking__ds__day = '2020-01-01'
