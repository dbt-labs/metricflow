-- Join to Custom Granularity Dataset
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  subq_4.martian_day AS metric_time__martian_day
FROM ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_4
ON
  subq_3.metric_time__day = subq_4.ds
GROUP BY
  subq_4.martian_day
