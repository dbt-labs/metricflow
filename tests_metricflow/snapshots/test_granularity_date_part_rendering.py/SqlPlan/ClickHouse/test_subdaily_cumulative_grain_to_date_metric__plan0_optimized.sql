test_name: test_subdaily_cumulative_grain_to_date_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_13.ts AS metric_time__hour
  , SUM(subq_11.__simple_subdaily_metric_default_day) AS subdaily_cumulative_grain_to_date_metric
FROM ***************************.mf_time_spine_hour subq_13
INNER JOIN (
  SELECT
    toStartOfHour(archived_at) AS metric_time__hour
    , 1 AS __simple_subdaily_metric_default_day
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_11
ON
  (
    subq_11.metric_time__hour <= subq_13.ts
  ) AND (
    subq_11.metric_time__hour >= toStartOfHour(subq_13.ts)
  )
GROUP BY
  subq_13.ts
