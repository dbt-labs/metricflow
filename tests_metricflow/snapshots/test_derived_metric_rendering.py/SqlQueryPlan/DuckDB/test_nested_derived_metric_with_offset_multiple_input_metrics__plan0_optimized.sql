-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__day, subq_26.metric_time__day) AS metric_time__day
    , MAX(subq_21.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_26.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_20.ds AS metric_time__day
      , subq_18.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM ***************************.mf_time_spine subq_20
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
      ) subq_17
    ) subq_18
    ON
      DATE_TRUNC('month', subq_20.ds) = subq_18.metric_time__day
  ) subq_21
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
    ) subq_25
  ) subq_26
  ON
    subq_21.metric_time__day = subq_26.metric_time__day
  GROUP BY
    COALESCE(subq_21.metric_time__day, subq_26.metric_time__day)
) subq_27
