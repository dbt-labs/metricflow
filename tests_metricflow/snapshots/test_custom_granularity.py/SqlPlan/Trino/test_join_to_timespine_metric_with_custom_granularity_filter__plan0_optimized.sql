test_name: test_join_to_timespine_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_21.metric_time__alien_day AS metric_time__alien_day
  , subq_17.bookings_join_to_time_spine AS bookings_join_to_time_spine
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__alien_day']
  SELECT
    metric_time__alien_day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      alien_day AS metric_time__alien_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_19
  WHERE metric_time__alien_day = '2020-01-01'
  GROUP BY
    metric_time__alien_day
) subq_21
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings_join_to_time_spine', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__alien_day
    , SUM(bookings_join_to_time_spine) AS bookings_join_to_time_spine
  FROM (
    -- Metric Time Dimension 'ds'
    -- Join to Custom Granularity Dataset
    SELECT
      subq_12.bookings_join_to_time_spine AS bookings_join_to_time_spine
      , subq_13.alien_day AS metric_time__alien_day
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      SELECT
        1 AS bookings_join_to_time_spine
        , DATE_TRUNC('day', ds) AS ds__day
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_13
    ON
      subq_12.ds__day = subq_13.ds
  ) subq_14
  WHERE metric_time__alien_day = '2020-01-01'
  GROUP BY
    metric_time__alien_day
) subq_17
ON
  subq_21.metric_time__alien_day = subq_17.metric_time__alien_day
