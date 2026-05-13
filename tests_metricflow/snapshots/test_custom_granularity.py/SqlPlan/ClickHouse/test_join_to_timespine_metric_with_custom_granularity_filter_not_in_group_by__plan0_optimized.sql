test_name: test_join_to_timespine_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  subq_25.metric_time__day AS metric_time__day
  , subq_20.__bookings_join_to_time_spine AS bookings_join_to_time_spine
FROM (
  SELECT
    metric_time__day
  FROM (
    SELECT
      ds AS metric_time__day
      , alien_day AS metric_time__alien_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_23
  WHERE metric_time__alien_day = '2020-01-02'
) subq_25
LEFT OUTER JOIN (
  SELECT
    metric_time__day
    , SUM(__bookings_join_to_time_spine) AS __bookings_join_to_time_spine
  FROM (
    SELECT
      metric_time__day
      , bookings_join_to_time_spine AS __bookings_join_to_time_spine
    FROM (
      SELECT
        subq_15.alien_day AS metric_time__alien_day
        , subq_14.ds__day AS metric_time__day
        , subq_14.__bookings_join_to_time_spine AS bookings_join_to_time_spine
      FROM (
        SELECT
          1 AS __bookings_join_to_time_spine
          , toStartOfDay(ds) AS ds__day
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_14
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_15
      ON
        subq_14.ds__day = subq_15.ds
    ) subq_17
    WHERE metric_time__alien_day = '2020-01-02'
  ) subq_19
  GROUP BY
    metric_time__day
) subq_20
ON
  subq_25.metric_time__day = subq_20.metric_time__day
