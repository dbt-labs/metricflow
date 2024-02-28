-- Calculate min and max
SELECT
  MIN(metric_time__day) AS metric_time__day__min
  , MAX(metric_time__day) AS metric_time__day__max
FROM (
  -- Time Spine
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['metric_time__day',]
  SELECT
    DATE_TRUNC(ds, day) AS metric_time__day
  FROM ***************************.mf_time_spine time_spine_src_28000
  GROUP BY
    metric_time__day
) subq_5
