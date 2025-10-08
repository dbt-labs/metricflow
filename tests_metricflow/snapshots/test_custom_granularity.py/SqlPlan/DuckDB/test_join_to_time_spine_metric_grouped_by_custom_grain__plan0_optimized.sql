test_name: test_join_to_time_spine_metric_grouped_by_custom_grain
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'metric_time__alien_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_7.alien_day AS metric_time__alien_day
  , SUM(subq_6.bookings) AS bookings_join_to_time_spine
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS bookings
    , DATE_TRUNC('day', ds) AS ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_6
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_7
ON
  subq_6.ds__day = subq_7.ds
GROUP BY
  subq_7.alien_day
