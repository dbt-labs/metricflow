test_name: test_subdaily_offset_window_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__hour
  , archived_users AS subdaily_offset_window_metric
FROM (
  SELECT
    time_spine_src_28005.ts AS metric_time__hour
    , subq_16.__archived_users AS archived_users
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  INNER JOIN (
    SELECT
      metric_time__hour
      , SUM(__archived_users) AS __archived_users
    FROM (
      SELECT
        toStartOfHour(archived_at) AS metric_time__hour
        , 1 AS __archived_users
      FROM ***************************.dim_users users_ds_source_src_28000
    ) subq_15
    GROUP BY
      metric_time__hour
  ) subq_16
  ON
    addHours(time_spine_src_28005.ts, -1) = subq_16.metric_time__hour
) subq_22
