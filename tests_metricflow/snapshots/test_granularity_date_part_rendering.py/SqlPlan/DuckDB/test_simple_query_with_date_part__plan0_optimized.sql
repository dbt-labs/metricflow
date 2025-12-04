test_name: test_simple_query_with_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__extract_dow
  , SUM(__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__bookings', 'metric_time__extract_dow']
  -- Pass Only Elements: ['__bookings', 'metric_time__extract_dow']
  SELECT
    EXTRACT(isodow FROM ds) AS metric_time__extract_dow
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_9
GROUP BY
  metric_time__extract_dow
