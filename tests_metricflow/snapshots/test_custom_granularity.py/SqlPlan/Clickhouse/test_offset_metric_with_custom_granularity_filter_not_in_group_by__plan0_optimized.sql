test_name: test_offset_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings_5_days_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Join to Custom Granularity Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_12.bookings AS bookings
      , subq_16.martian_day AS metric_time__martian_day
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
    ) subq_12
    ON
      addDays(time_spine_src_28006.ds, CAST(-5 AS Integer)) = subq_12.metric_time__day
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_16
    ON
      time_spine_src_28006.ds = subq_16.ds
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_17
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__day
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_21
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
