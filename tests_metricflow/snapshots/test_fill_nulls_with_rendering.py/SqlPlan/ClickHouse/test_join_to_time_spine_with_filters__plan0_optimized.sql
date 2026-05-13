test_name: test_join_to_time_spine_with_filters
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    subq_32.metric_time__day AS metric_time__day
    , subq_26.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM (
    SELECT
      metric_time__day
    FROM (
      SELECT
        ds AS metric_time__day
        , toStartOfWeek(ds, 1) AS metric_time__week
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_29
    WHERE (
      metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
    ) AND (
      metric_time__week > '2020-01-01'
    )
  ) subq_32
  LEFT OUTER JOIN (
    SELECT
      metric_time__day
      , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      SELECT
        metric_time__day
        , bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , toStartOfWeek(ds, 1) AS metric_time__week
          , 1 AS bookings_fill_nulls_with_0
        FROM ***************************.fct_bookings bookings_source_src_28000
        WHERE toStartOfDay(ds) BETWEEN '2020-01-03' AND '2020-01-05'
      ) subq_23
      WHERE metric_time__week > '2020-01-01'
    ) subq_25
    GROUP BY
      metric_time__day
  ) subq_26
  ON
    subq_32.metric_time__day = subq_26.metric_time__day
  WHERE subq_32.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
) subq_34
