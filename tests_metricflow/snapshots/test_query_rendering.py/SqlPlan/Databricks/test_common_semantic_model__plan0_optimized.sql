test_name: test_common_semantic_model
test_filename: test_query_rendering.py
sql_engine: Databricks
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(bookings) AS bookings
  , SUM(booking_value) AS booking_value
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'booking_value', 'metric_time__day']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
    , booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_12
GROUP BY
  metric_time__day
