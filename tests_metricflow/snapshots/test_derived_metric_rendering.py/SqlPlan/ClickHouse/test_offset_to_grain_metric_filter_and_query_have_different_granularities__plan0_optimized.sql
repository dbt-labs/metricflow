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
    subq_24.metric_time__month AS metric_time__month
    , subq_19.__bookings AS bookings_start_of_month
  FROM (
    SELECT
      metric_time__month
    FROM (
      SELECT
        ds AS metric_time__day
        , toStartOfMonth(ds) AS metric_time__month
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_22
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_24
  INNER JOIN (
    SELECT
      metric_time__month
      , SUM(__bookings) AS __bookings
    FROM (
      SELECT
        metric_time__month
        , bookings AS __bookings
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , toStartOfMonth(ds) AS metric_time__month
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_16
      WHERE metric_time__day = '2020-01-01'
    ) subq_18
    GROUP BY
      metric_time__month
  ) subq_19
  ON
    toStartOfMonth(subq_24.metric_time__month) = subq_19.metric_time__month
) subq_26
