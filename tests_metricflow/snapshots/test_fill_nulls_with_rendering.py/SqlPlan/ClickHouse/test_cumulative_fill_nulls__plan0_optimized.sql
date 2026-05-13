test_name: test_cumulative_fill_nulls
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , COALESCE(__bookers_fill_nulls_with_0_join_to_timespine, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_22.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    SELECT
      subq_18.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS __bookers_fill_nulls_with_0_join_to_timespine
    FROM ***************************.mf_time_spine subq_18
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        toStartOfDay(bookings_source_src_28000.ds) <= subq_18.ds
      ) AND (
        toStartOfDay(bookings_source_src_28000.ds) > addDays(subq_18.ds, -2)
      )
    GROUP BY
      subq_18.ds
  ) subq_22
  ON
    time_spine_src_28006.ds = subq_22.metric_time__day
) subq_27
