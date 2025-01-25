test_name: test_no_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__day', 'metric_time__martian_day', 'user__bio_added_ts__martian_day', 'user__bio_added_ts__month']
SELECT
  nr_subq_10.martian_day AS user__bio_added_ts__martian_day
  , nr_subq_9.martian_day AS metric_time__martian_day
  , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
  , time_spine_src_28006.ds AS metric_time__day
FROM ***************************.dim_users users_ds_source_src_28000
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine nr_subq_9
ON
  time_spine_src_28006.ds = nr_subq_9.ds
LEFT OUTER JOIN
  ***************************.mf_time_spine nr_subq_10
ON
  DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) = nr_subq_10.ds
GROUP BY
  nr_subq_10.martian_day
  , nr_subq_9.martian_day
  , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts)
  , time_spine_src_28006.ds
