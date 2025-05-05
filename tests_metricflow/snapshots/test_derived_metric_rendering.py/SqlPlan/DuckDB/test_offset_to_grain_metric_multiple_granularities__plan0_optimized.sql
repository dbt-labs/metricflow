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
  -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__month', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
    , DATE_TRUNC('year', time_spine_src_28006.ds) AS metric_time__year
    , SUM(subq_11.bookings) AS bookings_start_of_month
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_11
  ON
    DATE_TRUNC('month', time_spine_src_28006.ds) = subq_11.metric_time__day
  GROUP BY
    time_spine_src_28006.ds
    , DATE_TRUNC('month', time_spine_src_28006.ds)
    , DATE_TRUNC('year', time_spine_src_28006.ds)
) subq_18
