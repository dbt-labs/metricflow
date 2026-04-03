test_name: test_join_to_time_spine_metric_grouped_by_custom_grain
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  subq_21.metric_time__alien_day AS metric_time__alien_day
  , subq_17.__bookings_join_to_time_spine AS bookings_join_to_time_spine
FROM (
  SELECT
    alien_day AS metric_time__alien_day
  FROM ***************************.mf_time_spine time_spine_src_28006
  GROUP BY
    alien_day
) subq_21
LEFT OUTER JOIN (
  SELECT
    subq_13.alien_day AS metric_time__alien_day
    , SUM(subq_12.__bookings_join_to_time_spine) AS __bookings_join_to_time_spine
  FROM (
    SELECT
      1 AS __bookings_join_to_time_spine
      , toStartOfDay(ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_13
  ON
    subq_12.ds__day = subq_13.ds
  GROUP BY
    subq_13.alien_day
) subq_17
ON
  subq_21.metric_time__alien_day = subq_17.metric_time__alien_day
