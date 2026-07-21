test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  SELECT
    subq_20.metric_time__month AS metric_time__month
    , subq_16.__bookings_monthly AS bookings_last_month
  FROM (
    SELECT
      toStartOfMonth(ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_16006
    GROUP BY
      toStartOfMonth(ds)
  ) subq_20
  INNER JOIN (
    SELECT
      toStartOfMonth(ds) AS metric_time__month
      , SUM(bookings_monthly) AS __bookings_monthly
    FROM ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
    GROUP BY
      toStartOfMonth(ds)
  ) subq_16
  ON
    addMonths(subq_20.metric_time__month, -1) = subq_16.metric_time__month
) subq_22
