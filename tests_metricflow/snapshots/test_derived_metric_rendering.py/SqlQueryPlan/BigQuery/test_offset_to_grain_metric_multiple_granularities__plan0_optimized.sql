test_name: test_offset_to_grain_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with multiple granularities.
---
-- Compute Metrics via Expressions
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
    subq_11.ds AS metric_time__day
    , DATETIME_TRUNC(subq_11.ds, month) AS metric_time__month
    , DATETIME_TRUNC(subq_11.ds, year) AS metric_time__year
    , SUM(subq_9.bookings) AS bookings_start_of_month
  FROM ***************************.mf_time_spine subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  ON
    DATETIME_TRUNC(subq_11.ds, month) = subq_9.metric_time__day
  GROUP BY
    metric_time__day
    , metric_time__month
    , metric_time__year
) subq_15
