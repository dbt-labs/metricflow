test_name: test_derived_offset_metric_with_one_input_metric
test_filename: test_derived_metric_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , SUM(subq_10.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN
  (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_10
  ON
    addDays(time_spine_src_28006.ds, CAST(-5 AS Integer)) = subq_10.metric_time__day
  GROUP BY
    time_spine_src_28006.ds
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_17
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
