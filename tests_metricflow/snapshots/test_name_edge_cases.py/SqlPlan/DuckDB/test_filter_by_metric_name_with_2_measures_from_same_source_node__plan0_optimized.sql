test_name: test_filter_by_metric_name_with_2_measures_from_same_source_node
test_filename: test_name_edge_cases.py
docstring:
  Check a soon-to-be-deprecated use case of filtering by a metric name with 2 metrics from the same source node.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__homonymous_metric_and_entity', '__homonymous_metric_and_dimension', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(__homonymous_metric_and_entity) AS homonymous_metric_and_entity
  , SUM(__homonymous_metric_and_dimension) AS homonymous_metric_and_dimension
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
  FROM ***************************.fct_bookings bookings_source_src_32000
) subq_13
WHERE homonymous_metric_and_entity IS NOT NULL
GROUP BY
  metric_time__day
