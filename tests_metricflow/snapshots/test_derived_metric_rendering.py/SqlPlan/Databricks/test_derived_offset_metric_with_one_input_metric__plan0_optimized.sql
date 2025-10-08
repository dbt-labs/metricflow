test_name: test_derived_offset_metric_with_one_input_metric
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_13.bookings_5_days_ago AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings_5_days_ago
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    GROUP BY
      metric_time__day
  ) subq_13
  ON
    DATEADD(day, -5, time_spine_src_28006.ds) = subq_13.metric_time__day
) subq_18
