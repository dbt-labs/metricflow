test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  SELECT
    subq_40.metric_time__day AS metric_time__day
    , subq_35.__bookers AS every_2_days_bookers_2_days_ago
  FROM (
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    WHERE ds BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_40
  INNER JOIN (
    SELECT
      metric_time__day
      , COUNT(DISTINCT __bookers) AS __bookers
    FROM (
      SELECT
        subq_30.ds AS metric_time__day
        , bookings_source_src_28000.guest_id AS __bookers
      FROM ***************************.mf_time_spine subq_30
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          toStartOfDay(bookings_source_src_28000.ds) <= subq_30.ds
        ) AND (
          toStartOfDay(bookings_source_src_28000.ds) > addDays(subq_30.ds, -2)
        )
      WHERE subq_30.ds BETWEEN '2019-12-19' AND '2020-01-02'
    ) subq_34
    GROUP BY
      metric_time__day
  ) subq_35
  ON
    addDays(subq_40.metric_time__day, -2) = subq_35.metric_time__day
  WHERE subq_40.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
) subq_44
