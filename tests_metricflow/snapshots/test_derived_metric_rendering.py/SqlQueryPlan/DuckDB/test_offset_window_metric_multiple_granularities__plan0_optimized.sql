test_name: test_offset_window_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with multiple granularities.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , metric_time__month
  , metric_time__year
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_28.metric_time__day) AS metric_time__day
    , COALESCE(subq_23.metric_time__month, subq_28.metric_time__month) AS metric_time__month
    , COALESCE(subq_23.metric_time__year, subq_28.metric_time__year) AS metric_time__year
    , MAX(subq_23.booking_value) AS booking_value
    , MAX(subq_28.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
      , DATE_TRUNC('year', time_spine_src_28006.ds) AS metric_time__year
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      time_spine_src_28006.ds - INTERVAL 1 week = DATE_TRUNC('day', bookings_source_src_28000.ds)
    GROUP BY
      time_spine_src_28006.ds
      , DATE_TRUNC('month', time_spine_src_28006.ds)
      , DATE_TRUNC('year', time_spine_src_28006.ds)
  ) subq_23
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
      , DATE_TRUNC('year', ds) AS metric_time__year
      , COUNT(DISTINCT guest_id) AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      DATE_TRUNC('day', ds)
      , DATE_TRUNC('month', ds)
      , DATE_TRUNC('year', ds)
  ) subq_28
  ON
    (
      subq_23.metric_time__day = subq_28.metric_time__day
    ) AND (
      subq_23.metric_time__month = subq_28.metric_time__month
    ) AND (
      subq_23.metric_time__year = subq_28.metric_time__year
    )
  GROUP BY
    COALESCE(subq_23.metric_time__day, subq_28.metric_time__day)
    , COALESCE(subq_23.metric_time__month, subq_28.metric_time__month)
    , COALESCE(subq_23.metric_time__year, subq_28.metric_time__year)
) subq_29
