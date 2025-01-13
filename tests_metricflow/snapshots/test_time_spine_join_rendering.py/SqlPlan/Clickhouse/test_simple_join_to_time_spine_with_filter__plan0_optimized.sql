test_name: test_simple_join_to_time_spine_with_filter
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric fills nulls and filter is not in group by. Should apply constraint once.
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_13.bookings AS bookings
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN
  (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_10
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_13
  ON
    time_spine_src_28006.ds = subq_13.metric_time__day
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_17
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
