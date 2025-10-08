test_name: test_simple_join_to_time_spine
test_filename: test_time_spine_join_rendering.py
sql_engine: DuckDB
---
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(bookings) AS bookings_join_to_time_spine
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
GROUP BY
  metric_time__day
