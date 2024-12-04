test_name: test_derived_metric_with_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__week
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__week, subq_28.metric_time__week) AS metric_time__week
    , MAX(subq_19.bookings) AS bookings
    , MAX(subq_28.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__week
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__week']
      SELECT
        DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_17
    GROUP BY
      metric_time__week
  ) subq_19
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('week', time_spine_src_28006.ds) AS metric_time__week
      , SUM(subq_21.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_21
    ON
      DATE_TRUNC('month', time_spine_src_28006.ds) = subq_21.metric_time__day
    WHERE DATE_TRUNC('week', time_spine_src_28006.ds) = time_spine_src_28006.ds
    GROUP BY
      DATE_TRUNC('week', time_spine_src_28006.ds)
  ) subq_28
  ON
    subq_19.metric_time__week = subq_28.metric_time__week
  GROUP BY
    COALESCE(subq_19.metric_time__week, subq_28.metric_time__week)
) subq_29
