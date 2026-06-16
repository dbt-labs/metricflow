test_name: test_offset_metric_with_metric_time_filter
test_filename: test_offset_metrics_with_filters.py
sql_engine: DuckDB
expectation_description:
  The metric_time filter should be applied on the time spine / output side of the
  offset join, ideally by pushing it to the time spine before the join rather than
  into the pre-offset metric input.
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , 2 * bookings AS bookings_offset_once
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    subq_22.metric_time__day AS metric_time__day
    , subq_17.__bookings AS bookings
  FROM (
    -- Constrain Output with WHERE
    -- Select: ['metric_time__day']
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      -- Select: ['metric_time__day']
      SELECT
        ds AS metric_time__day
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_20
    WHERE metric_time__day = '2020-01-01' 
  ) subq_22
  INNER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(__bookings) AS __bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Select: ['__bookings', 'metric_time__day']
      -- Select: ['__bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_16
    GROUP BY
      metric_time__day
  ) subq_17
  ON
    subq_22.metric_time__day - INTERVAL 5 day = subq_17.metric_time__day
) subq_24
