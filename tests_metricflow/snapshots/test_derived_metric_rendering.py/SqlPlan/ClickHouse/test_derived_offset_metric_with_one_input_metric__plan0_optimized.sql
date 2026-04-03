test_name: test_derived_offset_metric_with_one_input_metric
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_16.__bookings AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    SELECT
      metric_time__day
      , SUM(__bookings) AS __bookings
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    GROUP BY
      metric_time__day
  ) subq_16
  ON
    addDays(time_spine_src_28006.ds, -5) = subq_16.metric_time__day
) subq_22
