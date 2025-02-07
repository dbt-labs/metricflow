test_name: test_simple_metric_with_custom_granularity_in_filter_and_group_by
test_filename: test_custom_granularity.py
docstring:
  Simple metric queried with a filter on a custom grain, where that grain is also used in the group by.
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__alien_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__alien_day
  , SUM(bookings) AS bookings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_6.bookings AS bookings
    , subq_7.alien_day AS metric_time__alien_day
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
) subq_8
WHERE metric_time__alien_day = '2020-01-01'
GROUP BY
  metric_time__alien_day
