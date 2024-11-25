test_name: test_offset_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: DuckDB
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
    subq_11.metric_time__day AS metric_time__day
    , SUM(subq_10.bookings) AS bookings_5_days_ago
  FROM (
    -- Filter Time Spine
    SELECT
      metric_time__day
    FROM (
      -- Time Spine
      SELECT
        martian_day AS metric_time__martian_day
        , ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_12
    ) subq_13
    WHERE metric_time__martian_day = '2020-01-01'
  ) subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  ON
    subq_11.metric_time__day - INTERVAL 5 day = subq_10.metric_time__day
  GROUP BY
    subq_11.metric_time__day
) subq_17
