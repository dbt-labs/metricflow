-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__day', 'metric_time__martian_day', 'user__bio_added_ts__martian_day', 'user__bio_added_ts__month']
SELECT
  DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
  , DATE_TRUNC('day', time_spine_src_28006.ds) AS metric_time__day
  , subq_8.martian_day AS metric_time__martian_day
  , subq_9.martian_day AS user__bio_added_ts__martian_day
FROM ***************************.dim_users users_ds_source_src_28000
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_8
ON
  DATE_TRUNC('day', time_spine_src_28006.ds) = subq_8.ds
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_9
ON
  DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) = subq_9.ds
GROUP BY
  DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts)
  , DATE_TRUNC('day', time_spine_src_28006.ds)
  , subq_8.martian_day
  , subq_9.martian_day
