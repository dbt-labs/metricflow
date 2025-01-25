test_name: test_offset_window_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with one granularity and filtered by a different one.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_23.metric_time__month, nr_subq_28.metric_time__month) AS metric_time__month
    , MAX(nr_subq_23.booking_value) AS booking_value
    , MAX(nr_subq_28.bookers) AS bookers
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , SUM(booking_value) AS booking_value
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        time_spine_src_28006.ds AS metric_time__day
        , DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
        , bookings_source_src_28000.booking_value AS booking_value
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        DATEADD(week, -1, time_spine_src_28006.ds) = DATE_TRUNC('day', bookings_source_src_28000.ds)
    ) nr_subq_19
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) nr_subq_23
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookers', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , COUNT(DISTINCT bookers) AS bookers
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('month', ds) AS metric_time__month
        , booking_value
        , guest_id AS bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_24
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) nr_subq_28
  ON
    nr_subq_23.metric_time__month = nr_subq_28.metric_time__month
  GROUP BY
    COALESCE(nr_subq_23.metric_time__month, nr_subq_28.metric_time__month)
) nr_subq_29
