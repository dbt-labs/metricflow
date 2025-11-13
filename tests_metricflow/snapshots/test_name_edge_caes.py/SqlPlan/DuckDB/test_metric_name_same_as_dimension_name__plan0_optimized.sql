test_name: test_metric_name_same_as_dimension_name
test_filename: test_name_edge_caes.py
docstring:
  Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as a dimension.
sql_engine: DuckDB
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__conflicted_name
  , SUM(__conflicted_name) AS conflicted_name
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__conflicted_name', 'booking__conflicted_name']
  SELECT
    'dummy_dimension_value' AS booking__conflicted_name
    , booking_value AS __conflicted_name
  FROM ***************************.fct_bookings bookings_source_src_32000
) subq_7
GROUP BY
  booking__conflicted_name
