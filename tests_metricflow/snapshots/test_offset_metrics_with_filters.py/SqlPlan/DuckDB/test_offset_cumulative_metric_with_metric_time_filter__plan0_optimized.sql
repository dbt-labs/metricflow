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
    subq_31.metric_time__day AS metric_time__day
    , subq_26.__bookings AS trailing_7_days_bookings_1_week_ago
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
    ) subq_29
    WHERE metric_time__day = '2020-01-01'
  ) subq_31
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Select: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings) AS __bookings
    FROM (
      -- Join Self Over Time Range
      -- Select: ['__bookings', 'metric_time__day']
      SELECT
        subq_21.ds AS metric_time__day
        , subq_19.__bookings AS bookings
      FROM ***************************.mf_time_spine subq_21
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_19
      ON
        (
          subq_19.metric_time__day <= subq_21.ds
        ) AND (
          subq_19.metric_time__day > subq_21.ds - INTERVAL 7 day
        )
    ) subq_23
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_26
  ON
    subq_31.metric_time__day - INTERVAL 1 week = subq_26.metric_time__day
) subq_34
