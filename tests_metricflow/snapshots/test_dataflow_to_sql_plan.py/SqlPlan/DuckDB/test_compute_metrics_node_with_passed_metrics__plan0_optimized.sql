test_name: test_compute_metrics_node_with_passed_metrics
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests SQL for a `ComputeMetricsNode` that passes a previously computed metric from the source subquery.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  booking_value
  , booking_value * 0.05 AS booking_fees
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['__booking_value']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
