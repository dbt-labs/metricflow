test_name: test_join_to_time_spine_metric_grouped_by_custom_grain
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_17.metric_time__alien_day AS metric_time__alien_day
  , subq_14.__bookings_join_to_time_spine AS bookings_join_to_time_spine
FROM (
  -- Read From Time Spine 'mf_time_spine'
  -- Change Column Aliases
  -- Pass Only Elements: ['metric_time__alien_day']
  SELECT
    alien_day AS metric_time__alien_day
  FROM ***************************.mf_time_spine time_spine_src_28006
  GROUP BY
    alien_day
) subq_17
LEFT OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__bookings_join_to_time_spine', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    subq_11.alien_day AS metric_time__alien_day
    , SUM(subq_10.__bookings_join_to_time_spine) AS __bookings_join_to_time_spine
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS __bookings_join_to_time_spine
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_11
  ON
    subq_10.ds__day = subq_11.ds
  GROUP BY
    subq_11.alien_day
) subq_14
ON
  subq_17.metric_time__alien_day = subq_14.metric_time__alien_day
