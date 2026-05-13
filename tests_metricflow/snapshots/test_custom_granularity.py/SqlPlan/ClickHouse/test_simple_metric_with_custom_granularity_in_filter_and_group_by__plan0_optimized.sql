test_name: test_simple_metric_with_custom_granularity_in_filter_and_group_by
test_filename: test_custom_granularity.py
docstring:
  Simple metric queried with a filter on a custom grain, where that grain is also used in the group by.
sql_engine: ClickHouse
---
SELECT
  metric_time__alien_day
  , SUM(__bookings) AS bookings
FROM (
  SELECT
    metric_time__alien_day
    , bookings AS __bookings
  FROM (
    SELECT
      subq_9.alien_day AS metric_time__alien_day
      , subq_8.__bookings AS bookings
    FROM (
      SELECT
        1 AS __bookings
        , toStartOfDay(ds) AS ds__day
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_8
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_9
    ON
      subq_8.ds__day = subq_9.ds
  ) subq_11
  WHERE metric_time__alien_day = '2020-01-01'
) subq_13
GROUP BY
  metric_time__alien_day
