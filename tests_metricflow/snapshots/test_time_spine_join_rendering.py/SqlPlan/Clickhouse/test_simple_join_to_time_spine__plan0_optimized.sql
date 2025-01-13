test_name: test_simple_join_to_time_spine
test_filename: test_time_spine_join_rendering.py
sql_engine: Clickhouse
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  time_spine_src_28006.ds AS metric_time__day
  , subq_11.bookings AS bookings_join_to_time_spine
FROM ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
(
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_10
  GROUP BY
    metric_time__day
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_11
ON
  time_spine_src_28006.ds = subq_11.metric_time__day
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
