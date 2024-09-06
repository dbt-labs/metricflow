-- Calculate min and max
SELECT
  MIN(metric_time__week) AS metric_time__week__min
  , MAX(metric_time__week) AS metric_time__week__max
FROM (
  -- Time Spine
  -- Metric Time Dimension 'ts'
  -- Pass Only Elements: ['metric_time__week',]
  SELECT
    DATE_TRUNC('week', ts) AS metric_time__week
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  GROUP BY
    DATE_TRUNC('week', ts)
) subq_5
