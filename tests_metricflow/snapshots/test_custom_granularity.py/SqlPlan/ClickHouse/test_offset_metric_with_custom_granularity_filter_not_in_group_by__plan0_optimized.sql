test_name: test_offset_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  SELECT
    subq_23.metric_time__day AS metric_time__day
    , subq_19.__bookings AS bookings_5_days_ago
  FROM (
    SELECT
      metric_time__day
    FROM (
      SELECT
        ds AS metric_time__day
        , alien_day AS metric_time__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_22
    WHERE metric_time__alien_day = '2020-01-01'
  ) subq_23
  INNER JOIN (
    SELECT
      subq_14.ds__day AS metric_time__day
      , SUM(subq_14.__bookings) AS __bookings
    FROM (
      SELECT
        1 AS __bookings
        , toStartOfDay(ds) AS ds__day
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_14
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_15
    ON
      subq_14.ds__day = subq_15.ds
    GROUP BY
      subq_14.ds__day
  ) subq_19
  ON
    addDays(subq_23.metric_time__day, -5) = subq_19.metric_time__day
) subq_26
SETTINGS join_use_nulls = 1
