test_name: test_simple_query_with_date_part
test_filename: test_granularity_date_part_rendering.py
---
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__extract_dow
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
  SELECT
    EXTRACT(dayofweekiso FROM ds) AS metric_time__extract_dow
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_6
GROUP BY
  metric_time__extract_dow
