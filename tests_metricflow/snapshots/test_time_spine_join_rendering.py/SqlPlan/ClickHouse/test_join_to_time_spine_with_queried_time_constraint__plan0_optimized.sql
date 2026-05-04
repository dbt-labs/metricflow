test_name: test_join_to_time_spine_with_queried_time_constraint
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric that fills nulls is queried with metric time and a time constraint. Should apply constraint twice.
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    subq_28.metric_time__day AS metric_time__day
    , subq_23.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM (
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    WHERE ds BETWEEN '2020-01-03' AND '2020-01-05'
  ) subq_28
  LEFT OUTER JOIN (
    SELECT
      metric_time__day
      , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , 1 AS __bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE toStartOfDay(ds) BETWEEN '2020-01-03' AND '2020-01-05'
    ) subq_22
    GROUP BY
      metric_time__day
  ) subq_23
  ON
    subq_28.metric_time__day = subq_23.metric_time__day
  WHERE subq_28.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
) subq_30
