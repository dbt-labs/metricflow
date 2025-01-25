test_name: test_simple_fill_nulls_with_0_month
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    nr_subq_12.metric_time__month AS metric_time__month
    , nr_subq_9.bookings AS bookings
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__month',]
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
    GROUP BY
      DATE_TRUNC('month', ds)
  ) nr_subq_12
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
        DATE_TRUNC('month', ds) AS metric_time__month
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_8
    GROUP BY
      metric_time__month
  ) nr_subq_9
  ON
    nr_subq_12.metric_time__month = nr_subq_9.metric_time__month
) nr_subq_13
