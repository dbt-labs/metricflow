test_name: test_simple_fill_nulls_with_0_month
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__month
  , SUM(bookings) AS bookings_fill_nulls_with_0
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__month']
  SELECT
    DATE_TRUNC('month', ds) AS metric_time__month
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
GROUP BY
  metric_time__month
