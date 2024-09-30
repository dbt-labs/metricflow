-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  time_spine_src_28006.martian_day AS metric_time__martian_day
  , subq_3.martian_day AS metric_time__martian_day
FROM ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_3
ON
  DATE_TRUNC('day', time_spine_src_28006.ds) = subq_3.ds
GROUP BY
  time_spine_src_28006.martian_day
  , subq_3.martian_day
