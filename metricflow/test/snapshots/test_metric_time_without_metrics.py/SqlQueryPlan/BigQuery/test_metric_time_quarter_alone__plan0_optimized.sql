-- Time Spine
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__quarter',]
SELECT
  DATE_TRUNC(ds, quarter) AS metric_time__quarter
FROM ***************************.mf_time_spine time_spine_src_10000
GROUP BY
  metric_time__quarter
