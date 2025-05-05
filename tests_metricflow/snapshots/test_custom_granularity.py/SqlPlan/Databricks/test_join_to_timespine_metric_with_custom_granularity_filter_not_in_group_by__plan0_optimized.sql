test_name: test_join_to_timespine_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_21.metric_time__day AS metric_time__day
  , subq_17.bookings AS bookings_join_to_time_spine
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      ds AS metric_time__day
      , alien_day AS metric_time__alien_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_19
  WHERE metric_time__alien_day = '2020-01-02'
) subq_21
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Metric Time Dimension 'ds'
    -- Join to Custom Granularity Dataset
    SELECT
      subq_12.ds__day AS metric_time__day
      , subq_12.bookings AS bookings
      , subq_13.alien_day AS metric_time__alien_day
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      SELECT
        1 AS bookings
        , DATE_TRUNC('day', ds) AS ds__day
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_13
    ON
      subq_12.ds__day = subq_13.ds
  ) subq_14
  WHERE metric_time__alien_day = '2020-01-02'
  GROUP BY
    metric_time__day
) subq_17
ON
  subq_21.metric_time__day = subq_17.metric_time__day
