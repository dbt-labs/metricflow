test_name: test_derived_offset_cumulative_metric
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_23.__bookers AS every_2_days_bookers_2_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    SELECT
      subq_19.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS __bookers
    FROM ***************************.mf_time_spine subq_19
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        toStartOfDay(bookings_source_src_28000.ds) <= subq_19.ds
      ) AND (
        toStartOfDay(bookings_source_src_28000.ds) > addDays(subq_19.ds, -2)
      )
    GROUP BY
      subq_19.ds
  ) subq_23
  ON
    addDays(time_spine_src_28006.ds, -2) = subq_23.metric_time__day
) subq_30
