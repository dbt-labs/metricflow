test_name: test_offset_to_grain_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with multiple granularities.
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , metric_time__month
  , metric_time__year
  , bookings_start_of_month AS bookings_at_start_of_month
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , toStartOfMonth(time_spine_src_28006.ds) AS metric_time__month
    , toStartOfYear(time_spine_src_28006.ds) AS metric_time__year
    , subq_16.__bookings AS bookings_start_of_month
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    SELECT
      metric_time__day
      , metric_time__month
      , metric_time__year
      , SUM(__bookings) AS __bookings
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , toStartOfMonth(ds) AS metric_time__month
        , toStartOfYear(ds) AS metric_time__year
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_16
  ON
    toStartOfMonth(time_spine_src_28006.ds) = subq_16.metric_time__day
) subq_22
