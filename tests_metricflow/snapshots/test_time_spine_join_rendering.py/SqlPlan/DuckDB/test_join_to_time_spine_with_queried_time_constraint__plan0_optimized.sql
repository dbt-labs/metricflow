test_name: test_join_to_time_spine_with_queried_time_constraint
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric that fills nulls is queried with metric time and a time constraint. Should apply constraint twice.
sql_engine: DuckDB
---
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(bookings) AS bookings_fill_nulls_with_0
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
  WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-03' AND '2020-01-05'
) subq_10
GROUP BY
  metric_time__day
