-- Time Spine
-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__quarter',]
SELECT
  DATE_TRUNC('quarter', ts) AS metric_time__quarter
FROM ***************************.mf_time_spine_hour time_spine_src_28005
GROUP BY
  DATE_TRUNC('quarter', ts)
