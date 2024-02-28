-- Calculate min and max
SELECT
  MIN(metric_time__week) AS metric_time__week__min
  , MAX(metric_time__week) AS metric_time__week__max
FROM (
  -- Time Spine
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['metric_time__week',]
  SELECT
    DATE_TRUNC(ds, isoweek) AS metric_time__week
  FROM ***************************.mf_time_spine time_spine_src_28000
  GROUP BY
    metric_time__week
) subq_5
