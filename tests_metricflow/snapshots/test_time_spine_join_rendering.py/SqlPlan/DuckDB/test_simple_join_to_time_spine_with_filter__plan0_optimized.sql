test_name: test_simple_join_to_time_spine_with_filter
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric fills nulls and filter is not in group by. Should apply constraint once.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(bookings) AS bookings_fill_nulls_with_0
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , is_instant AS booking__is_instant
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
WHERE booking__is_instant
GROUP BY
  metric_time__day
