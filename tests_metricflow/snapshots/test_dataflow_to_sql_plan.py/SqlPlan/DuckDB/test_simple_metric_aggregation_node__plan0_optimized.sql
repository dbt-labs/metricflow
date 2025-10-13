test_name: test_simple_metric_aggregation_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf simple-metric input aggregation node.

      Covers SUM, AVERAGE, SUM_BOOLEAN (transformed to SUM upstream), and COUNT_DISTINCT agg types
sql_engine: DuckDB
---
-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements: ['bookings', 'instant_bookings', 'average_booking_value', 'bookers']
-- Aggregate Inputs for Simple Metrics
SELECT
  SUM(1) AS bookings
  , AVG(booking_value) AS average_booking_value
  , SUM(CASE WHEN is_instant THEN 1 ELSE 0 END) AS instant_bookings
  , COUNT(DISTINCT guest_id) AS bookers
FROM ***************************.fct_bookings bookings_source_src_28000
