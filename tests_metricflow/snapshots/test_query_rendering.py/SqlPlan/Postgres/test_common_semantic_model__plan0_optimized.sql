test_name: test_common_semantic_model
test_filename: test_query_rendering.py
sql_engine: Postgres
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(__bookings) AS bookings
  , SUM(__booking_value) AS booking_value
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__bookings', '__booking_value', 'metric_time__day']
  -- Pass Only Elements: ['__bookings', '__booking_value', 'metric_time__day']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS __bookings
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_15
GROUP BY
  metric_time__day
