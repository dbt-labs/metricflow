test_name: test_filter_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node.
---
-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements: ['bookings',]
SELECT
  1 AS bookings
FROM ***************************.fct_bookings bookings_source_src_28000
