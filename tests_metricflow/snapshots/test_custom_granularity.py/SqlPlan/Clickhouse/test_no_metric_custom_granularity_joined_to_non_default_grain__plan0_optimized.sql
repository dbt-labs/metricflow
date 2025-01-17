test_name: test_no_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Clickhouse
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__day', 'metric_time__martian_day', 'user__bio_added_ts__martian_day', 'user__bio_added_ts__month']
SELECT
  subq_12.martian_day AS user__bio_added_ts__martian_day
  , subq_11.martian_day AS metric_time__martian_day
  , date_trunc('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
  , time_spine_src_28006.ds AS metric_time__day
FROM ***************************.dim_users users_ds_source_src_28000
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_11
ON
  time_spine_src_28006.ds = subq_11.ds
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_12
ON
  date_trunc('day', users_ds_source_src_28000.bio_added_ts) = subq_12.ds
GROUP BY
  user__bio_added_ts__martian_day
  , metric_time__martian_day
  , user__bio_added_ts__month
  , metric_time__day
