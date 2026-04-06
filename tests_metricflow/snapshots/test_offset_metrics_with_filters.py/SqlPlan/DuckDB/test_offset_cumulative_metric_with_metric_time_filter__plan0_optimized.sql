test_name: test_offset_cumulative_metric_with_metric_time_filter
test_filename: test_offset_metrics_with_filters.py
docstring:
  Tests querying a cumulative metric that is offset with a filter on metric time.
sql_engine: DuckDB
expectation_description:
  The metric_time filter should be applied on the time spine / output side of the
  cumulative offset, ideally by pushing it to the time spine before the join
  rather than inside the pre-offset cumulative input.
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , trailing_7_days_bookings_1_week_ago AS trailing_7_days_bookings_offset_1_week
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  -- Compute Metrics via Expressions
  SELECT
    subq_29.metric_time__day AS metric_time__day
    , subq_24.__bookings AS trailing_7_days_bookings_1_week_ago
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
    ) subq_27
    WHERE metric_time__day = '2020-01-01'
  ) subq_29
  INNER JOIN (
    -- Join Self Over Time Range
    -- Select: ['__bookings', 'metric_time__day']
    -- Select: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_20.ds AS metric_time__day
      , SUM(subq_18.__bookings) AS __bookings
    FROM ***************************.mf_time_spine subq_20
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_18
    ON
      (
        subq_18.metric_time__day <= subq_20.ds
      ) AND (
        subq_18.metric_time__day > subq_20.ds - INTERVAL 7 day
      )
    GROUP BY
      subq_20.ds
  ) subq_24
  ON
    subq_29.metric_time__day - INTERVAL 1 week = subq_24.metric_time__day
) subq_32
