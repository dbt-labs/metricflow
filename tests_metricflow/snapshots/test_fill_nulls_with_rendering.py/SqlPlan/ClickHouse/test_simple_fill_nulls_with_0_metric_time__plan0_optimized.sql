test_name: test_simple_fill_nulls_with_0_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_15.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    SELECT
      metric_time__day
      , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , 1 AS __bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_14
    GROUP BY
      metric_time__day
  ) subq_15
  ON
    time_spine_src_28006.ds = subq_15.metric_time__day
) subq_20
