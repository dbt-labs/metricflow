test_name: test_subdaily_time_constraint_with_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_28.metric_time__hour AS metric_time__hour
  , subq_23.__subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
FROM (
  SELECT
    ts AS metric_time__hour
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  WHERE ts BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
) subq_28
LEFT OUTER JOIN (
  SELECT
    metric_time__hour
    , SUM(__subdaily_join_to_time_spine_metric) AS __subdaily_join_to_time_spine_metric
  FROM (
    SELECT
      toStartOfHour(archived_at) AS metric_time__hour
      , 1 AS __subdaily_join_to_time_spine_metric
    FROM ***************************.dim_users users_ds_source_src_28000
    WHERE toStartOfHour(archived_at) BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
  ) subq_22
  GROUP BY
    metric_time__hour
) subq_23
ON
  subq_28.metric_time__hour = subq_23.metric_time__hour
WHERE subq_28.metric_time__hour BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
