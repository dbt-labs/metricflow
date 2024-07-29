-- Time Spine
-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__millisecond',]
SELECT
  DATETIME_TRUNC(ts, millisecond) AS metric_time__millisecond
FROM ***************************.mf_time_spine_millisecond time_spine_src_28002
GROUP BY
  metric_time__millisecond
