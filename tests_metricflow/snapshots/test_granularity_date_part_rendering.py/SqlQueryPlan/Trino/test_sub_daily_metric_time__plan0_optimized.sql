-- Time Spine
-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__millisecond',]
SELECT
  DATE_TRUNC('millisecond', ts) AS metric_time__millisecond
FROM ***************************.mf_time_spine_millisecond time_spine_src_28002
GROUP BY
  DATE_TRUNC('millisecond', ts)
