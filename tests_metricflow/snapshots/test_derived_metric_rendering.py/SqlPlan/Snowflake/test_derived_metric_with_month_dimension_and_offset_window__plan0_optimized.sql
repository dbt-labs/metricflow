test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    subq_20.metric_time__month AS metric_time__month
    , subq_16.__bookings_monthly AS bookings_last_month
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__month']
    -- Pass Only Elements: ['metric_time__month']
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_16006
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_20
  INNER JOIN (
    -- Read Elements From Semantic Model 'monthly_bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__bookings_monthly', 'metric_time__month']
    -- Pass Only Elements: ['__bookings_monthly', 'metric_time__month']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
      , SUM(bookings_monthly) AS __bookings_monthly
    FROM ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_16
  ON
    DATEADD(month, -1, subq_20.metric_time__month) = subq_16.metric_time__month
) subq_22
