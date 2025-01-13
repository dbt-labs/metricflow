test_name: test_simple_fill_nulls_with_0_month
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_14.metric_time__month AS metric_time__month
    , subq_11.bookings AS bookings
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__month',]
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
    GROUP BY
      DATE_TRUNC('month', ds)
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_14
  LEFT OUTER JOIN
  (
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
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_10
    GROUP BY
      metric_time__month
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_11
  ON
    subq_14.metric_time__month = subq_11.metric_time__month
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_15
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
