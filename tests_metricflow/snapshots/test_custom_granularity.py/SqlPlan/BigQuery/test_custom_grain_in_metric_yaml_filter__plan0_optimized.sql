test_name: test_custom_grain_in_metric_yaml_filter
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS bookings_on_first_alien_day
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_6.ds__day AS metric_time__day
    , subq_6.bookings AS bookings
    , subq_7.alien_day AS metric_time__alien_day
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATETIME_TRUNC(ds, day) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_6
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_7
  ON
    subq_6.ds__day = subq_7.ds
) subq_8
WHERE metric_time__alien_day
GROUP BY
  metric_time__day
