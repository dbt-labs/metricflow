test_name: test_simple_fill_nulls_with_0_month
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__month
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    subq_19.metric_time__month AS metric_time__month
    , subq_15.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM (
    SELECT
      toStartOfMonth(ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
    GROUP BY
      toStartOfMonth(ds)
  ) subq_19
  LEFT OUTER JOIN (
    SELECT
      metric_time__month
      , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      SELECT
        toStartOfMonth(ds) AS metric_time__month
        , 1 AS __bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_14
    GROUP BY
      metric_time__month
  ) subq_15
  ON
    subq_19.metric_time__month = subq_15.metric_time__month
) subq_20
