-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__day, subq_30.metric_time__day) AS metric_time__day
    , MAX(subq_24.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_30.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_23.ds AS metric_time__day
      , subq_21.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM ***************************.mf_time_spine subq_23
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking_value * 0.05 AS booking_fees_start_of_month
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , SUM(booking_value) AS booking_value
        FROM ***************************.fct_bookings bookings_source_src_10001
        GROUP BY
          DATE_TRUNC('day', ds)
      ) subq_20
    ) subq_21
    ON
      DATE_TRUNC('month', subq_23.ds) = subq_21.metric_time__day
  ) subq_24
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , booking_value * 0.05 AS booking_fees
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['booking_value', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , SUM(booking_value) AS booking_value
      FROM ***************************.fct_bookings bookings_source_src_10001
      GROUP BY
        DATE_TRUNC('day', ds)
    ) subq_29
  ) subq_30
  ON
    subq_24.metric_time__day = subq_30.metric_time__day
  GROUP BY
    COALESCE(subq_24.metric_time__day, subq_30.metric_time__day)
) subq_31
