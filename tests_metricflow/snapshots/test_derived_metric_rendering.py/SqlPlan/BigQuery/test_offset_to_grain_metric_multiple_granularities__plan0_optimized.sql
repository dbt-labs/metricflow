test_name: test_offset_to_grain_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with multiple granularities.
sql_engine: BigQuery
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
    , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS metric_time__month
    , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS metric_time__year
    , subq_13.bookings_start_of_month AS bookings_start_of_month
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , metric_time__month
      , metric_time__year
      , SUM(bookings) AS bookings_start_of_month
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__month', 'metric_time__year']
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , DATETIME_TRUNC(ds, month) AS metric_time__month
        , DATETIME_TRUNC(ds, year) AS metric_time__year
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_13
  ON
    DATETIME_TRUNC(time_spine_src_28006.ds, month) = subq_13.metric_time__day
) subq_18
