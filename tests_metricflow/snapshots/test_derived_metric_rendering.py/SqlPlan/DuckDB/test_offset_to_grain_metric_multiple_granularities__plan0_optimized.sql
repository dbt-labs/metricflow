test_name: test_offset_to_grain_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with multiple granularities.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , metric_time__month
  , metric_time__year
  , bookings_start_of_month AS bookings_at_start_of_month
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
    , DATE_TRUNC('year', time_spine_src_28006.ds) AS metric_time__year
    , subq_16.__bookings AS bookings_start_of_month
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , metric_time__month
      , metric_time__year
      , SUM(__bookings) AS __bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__bookings', 'metric_time__day', 'metric_time__month', 'metric_time__year']
      -- Pass Only Elements: ['__bookings', 'metric_time__day', 'metric_time__month', 'metric_time__year']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('month', ds) AS metric_time__month
        , DATE_TRUNC('year', ds) AS metric_time__year
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_16
  ON
    DATE_TRUNC('month', time_spine_src_28006.ds) = subq_16.metric_time__day
) subq_22
