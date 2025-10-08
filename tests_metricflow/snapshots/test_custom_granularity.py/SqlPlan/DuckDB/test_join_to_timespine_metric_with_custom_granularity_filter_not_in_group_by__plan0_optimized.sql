test_name: test_join_to_timespine_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(bookings) AS bookings_join_to_time_spine
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_7.ds__day AS metric_time__day
    , subq_7.bookings AS bookings
    , subq_8.alien_day AS metric_time__alien_day
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_7
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_8
  ON
    subq_7.ds__day = subq_8.ds
) subq_9
WHERE metric_time__alien_day = '2020-01-02'
GROUP BY
  metric_time__day
