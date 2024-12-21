test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_13.metric_time__month AS metric_time__month
    , SUM(monthly_bookings_source_src_16000.bookings_monthly) AS bookings_last_month
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__month',]
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_16006
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_13
  INNER JOIN
    ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
  ON
    DATE_ADD('month', -1, subq_13.metric_time__month) = DATE_TRUNC('month', monthly_bookings_source_src_16000.ds)
  GROUP BY
    subq_13.metric_time__month
) subq_17
