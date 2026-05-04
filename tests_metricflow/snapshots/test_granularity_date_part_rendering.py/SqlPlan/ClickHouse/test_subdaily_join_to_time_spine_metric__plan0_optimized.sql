test_name: test_subdaily_join_to_time_spine_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  time_spine_src_28005.ts AS metric_time__hour
  , subq_15.__subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
FROM ***************************.mf_time_spine_hour time_spine_src_28005
LEFT OUTER JOIN (
  SELECT
    metric_time__hour
    , SUM(__subdaily_join_to_time_spine_metric) AS __subdaily_join_to_time_spine_metric
  FROM (
    SELECT
      toStartOfHour(archived_at) AS metric_time__hour
      , 1 AS __subdaily_join_to_time_spine_metric
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_14
  GROUP BY
    metric_time__hour
) subq_15
ON
  time_spine_src_28005.ts = subq_15.metric_time__hour
