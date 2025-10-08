test_name: test_simple_fill_nulls_with_0_month
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__month
  , COALESCE(bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_15.metric_time__month AS metric_time__month
    , subq_12.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__month']
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_15
  LEFT OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__month
      , SUM(bookings_fill_nulls_with_0) AS bookings_fill_nulls_with_0
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings_fill_nulls_with_0', 'metric_time__month']
      SELECT
        DATE_TRUNC('month', ds) AS metric_time__month
        , 1 AS bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    GROUP BY
      metric_time__month
  ) subq_12
  ON
    subq_15.metric_time__month = subq_12.metric_time__month
) subq_16
