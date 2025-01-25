test_name: test_offset_window_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with multiple granularities.
sql_engine: Postgres
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
    COALESCE(nr_subq_20.metric_time__day, nr_subq_24.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_20.metric_time__month, nr_subq_24.metric_time__month) AS metric_time__month
    , COALESCE(nr_subq_20.metric_time__year, nr_subq_24.metric_time__year) AS metric_time__year
    , MAX(nr_subq_20.booking_value) AS booking_value
    , MAX(nr_subq_24.bookers) AS bookers
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
      time_spine_src_28006.ds - MAKE_INTERVAL(weeks => 1) = DATE_TRUNC('day', bookings_source_src_28000.ds)
    GROUP BY
      time_spine_src_28006.ds
      , DATE_TRUNC('month', time_spine_src_28006.ds)
      , DATE_TRUNC('year', time_spine_src_28006.ds)
  ) nr_subq_20
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
  ) nr_subq_24
  ON
    (
      nr_subq_20.metric_time__day = nr_subq_24.metric_time__day
    ) AND (
      nr_subq_20.metric_time__month = nr_subq_24.metric_time__month
    ) AND (
      nr_subq_20.metric_time__year = nr_subq_24.metric_time__year
    )
  GROUP BY
    COALESCE(nr_subq_20.metric_time__day, nr_subq_24.metric_time__day)
    , COALESCE(nr_subq_20.metric_time__month, nr_subq_24.metric_time__month)
    , COALESCE(nr_subq_20.metric_time__year, nr_subq_24.metric_time__year)
) nr_subq_25
