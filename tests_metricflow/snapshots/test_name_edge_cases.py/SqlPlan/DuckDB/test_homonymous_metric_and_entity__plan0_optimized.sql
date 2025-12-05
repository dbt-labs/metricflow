test_name: test_homonymous_metric_and_entity
test_filename: test_name_edge_cases.py
docstring:
  Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as an entity.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__homonymous_metric_and_entity', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(homonymous_metric_and_entity) AS homonymous_metric_and_entity
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__homonymous_metric_and_entity', 'metric_time__day']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , booking_value AS homonymous_metric_and_entity
  FROM ***************************.fct_bookings bookings_source_src_32000
) subq_9
WHERE homonymous_metric_and_entity IS NOT NULL
GROUP BY
  metric_time__day
