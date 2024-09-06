-- Calculate min and max
SELECT
  MIN(metric_time__day) AS metric_time__day__min
  , MAX(metric_time__day) AS metric_time__day__max
FROM (
  -- Time Spine
  -- Metric Time Dimension 'ts'
  -- Pass Only Elements: ['metric_time__day',]
  SELECT
    DATE_TRUNC('day', ts) AS metric_time__day
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  GROUP BY
    DATE_TRUNC('day', ts)
) subq_5
