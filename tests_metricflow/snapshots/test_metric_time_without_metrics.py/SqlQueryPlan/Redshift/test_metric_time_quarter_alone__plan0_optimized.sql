-- Time Spine
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__quarter',]
SELECT
  DATE_TRUNC('quarter', ds) AS metric_time__quarter
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  DATE_TRUNC('quarter', ds)
