test_name: test_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  SELECT
    subq_25.metric_time__day AS metric_time__day
    , subq_20.__bookings AS bookings_5_days_ago
  FROM (
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    WHERE ds BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_25
  INNER JOIN (
    SELECT
      metric_time__day
      , SUM(__bookings) AS __bookings
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_19
    GROUP BY
      metric_time__day
  ) subq_20
  ON
    addDays(subq_25.metric_time__day, -5) = subq_20.metric_time__day
  WHERE subq_25.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
) subq_28
