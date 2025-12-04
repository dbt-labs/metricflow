test_name: test_metric_name_same_as_dimension_name
test_filename: test_name_edge_cases.py
docstring:
  Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as a dimension.
sql_engine: DuckDB
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__homonymous_metric_and_dimension
  , SUM(__homonymous_metric_and_dimension) AS homonymous_metric_and_dimension
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__homonymous_metric_and_dimension', 'booking__homonymous_metric_and_dimension']
  -- Pass Only Elements: ['__homonymous_metric_and_dimension', 'booking__homonymous_metric_and_dimension']
  SELECT
    'dummy_dimension_value' AS booking__homonymous_metric_and_dimension
    , booking_value AS __homonymous_metric_and_dimension
  FROM ***************************.fct_bookings bookings_source_src_32000
) subq_9
GROUP BY
  booking__homonymous_metric_and_dimension
