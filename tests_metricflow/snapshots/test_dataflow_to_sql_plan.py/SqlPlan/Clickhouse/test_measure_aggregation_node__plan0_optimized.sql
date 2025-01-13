test_name: test_measure_aggregation_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf measure aggregation node.

      Covers SUM, AVERAGE, SUM_BOOLEAN (transformed to SUM upstream), and COUNT_DISTINCT agg types
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements: ['bookings', 'instant_bookings', 'average_booking_value', 'bookers']
-- Aggregate Measures
SELECT
  SUM(1) AS bookings
  , SUM(CASE WHEN is_instant THEN 1 ELSE 0 END) AS instant_bookings
  , COUNT(DISTINCT guest_id) AS bookers
  , AVG(booking_value) AS average_booking_value
FROM ***************************.fct_bookings bookings_source_src_28000
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
