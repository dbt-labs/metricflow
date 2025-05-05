test_name: test_simple_fill_nulls_with_0_month
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__month
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_15.metric_time__month AS metric_time__month
    , subq_12.bookings AS bookings
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__month']
    SELECT
      DATETIME_TRUNC(ds, month) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
    GROUP BY
      metric_time__month
  ) subq_15
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      metric_time__month
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__month']
      SELECT
        DATETIME_TRUNC(ds, month) AS metric_time__month
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    GROUP BY
      metric_time__month
  ) subq_12
  ON
    subq_15.metric_time__month = subq_12.metric_time__month
) subq_16
