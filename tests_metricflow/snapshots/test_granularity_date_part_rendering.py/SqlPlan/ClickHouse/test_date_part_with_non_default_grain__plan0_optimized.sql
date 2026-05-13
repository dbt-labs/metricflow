test_name: test_date_part_with_non_default_grain
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__extract_year
  , SUM(__archived_users) AS archived_users
FROM (
  SELECT
    metric_time__extract_year
    , archived_users AS __archived_users
  FROM (
    SELECT
      toYear(archived_at) AS metric_time__extract_year
      , toDayOfMonth(archived_at) AS metric_time__extract_day
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_9
  WHERE metric_time__extract_day = '2020-01-01'
) subq_11
GROUP BY
  metric_time__extract_year
