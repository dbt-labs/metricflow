test_name: test_no_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__day', 'metric_time__martian_day', 'user__bio_added_ts__martian_day', 'user__bio_added_ts__month']
SELECT
  subq_12.martian_day AS user__bio_added_ts__martian_day
  , subq_11.martian_day AS metric_time__martian_day
  , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
  , DATE_TRUNC('day', time_spine_src_28006.ds) AS metric_time__day
FROM ***************************.dim_users users_ds_source_src_28000
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_11
ON
  DATE_TRUNC('day', time_spine_src_28006.ds) = subq_11.ds
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_12
ON
  DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) = subq_12.ds
GROUP BY
  subq_12.martian_day
  , subq_11.martian_day
  , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts)
  , DATE_TRUNC('day', time_spine_src_28006.ds)
