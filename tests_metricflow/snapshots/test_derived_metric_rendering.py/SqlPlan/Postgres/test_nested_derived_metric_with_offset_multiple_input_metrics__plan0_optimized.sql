test_name: test_nested_derived_metric_with_offset_multiple_input_metrics
test_filename: test_derived_metric_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_23.metric_time__day, nr_subq_28.metric_time__day) AS metric_time__day
    , MAX(nr_subq_23.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(nr_subq_28.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , nr_subq_19.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking_value * 0.05 AS booking_fees_start_of_month
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['booking_value', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , SUM(booking_value) AS booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
        GROUP BY
          DATE_TRUNC('day', ds)
      ) nr_subq_18
    ) nr_subq_19
    ON
      DATE_TRUNC('month', time_spine_src_28006.ds) = nr_subq_19.metric_time__day
  ) nr_subq_23
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , booking_value * 0.05 AS booking_fees
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['booking_value', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , SUM(booking_value) AS booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
      GROUP BY
        DATE_TRUNC('day', ds)
    ) nr_subq_27
  ) nr_subq_28
  ON
    nr_subq_23.metric_time__day = nr_subq_28.metric_time__day
  GROUP BY
    COALESCE(nr_subq_23.metric_time__day, nr_subq_28.metric_time__day)
) nr_subq_29
