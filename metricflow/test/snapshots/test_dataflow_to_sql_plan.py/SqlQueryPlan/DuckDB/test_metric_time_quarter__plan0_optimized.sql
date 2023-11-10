-- Date Spine
-- Metric Time Dimension 'ds'
-- Pass Only Elements:
--   ['metric_time__year']
SELECT
  DATE_TRUNC('year', ds) AS metric_time__year
FROM ***************************.mf_time_spine time_spine_src_0
GROUP BY
  DATE_TRUNC('year', ds)
