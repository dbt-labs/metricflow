test_name: test_offset_to_grain_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with one granularity and filtered by a different one.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__month
  , bookings_start_of_month AS bookings_at_start_of_month
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    subq_24.metric_time__month AS metric_time__month
    , subq_19.__bookings AS bookings_start_of_month
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['metric_time__month']
    SELECT
      metric_time__month
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      -- Pass Only Elements: ['metric_time__month', 'metric_time__day']
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('month', ds) AS metric_time__month
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_22
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_24
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__bookings', 'metric_time__month']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__month
      , SUM(bookings) AS __bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__bookings', 'metric_time__month', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('month', ds) AS metric_time__month
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_16
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_19
  ON
    DATE_TRUNC('month', subq_24.metric_time__month) = subq_19.metric_time__month
) subq_26
