test_name: test_offset_to_grain_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with one granularity and filtered by a different one.
sql_engine: ClickHouse
---
SELECT
  metric_time__month
  , bookings_start_of_month AS bookings_at_start_of_month
FROM (
  SELECT
    subq_22.metric_time__month AS metric_time__month
    , subq_17.__bookings AS bookings_start_of_month
  FROM (
    SELECT
      metric_time__month
    FROM (
      SELECT
        metric_time__month
      FROM (
        SELECT
          ds AS metric_time__day
          , toStartOfMonth(ds) AS metric_time__month
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_20
      WHERE metric_time__day = '2020-01-01'
    ) subq_21
    GROUP BY
      metric_time__month
  ) subq_22
  INNER JOIN (
    SELECT
      metric_time__month
      , SUM(__bookings) AS __bookings
    FROM (
      SELECT
        toStartOfMonth(ds) AS metric_time__month
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_16
    GROUP BY
      metric_time__month
  ) subq_17
  ON
    toStartOfMonth(subq_22.metric_time__month) = subq_17.metric_time__month
) subq_24
SETTINGS join_use_nulls = 1
