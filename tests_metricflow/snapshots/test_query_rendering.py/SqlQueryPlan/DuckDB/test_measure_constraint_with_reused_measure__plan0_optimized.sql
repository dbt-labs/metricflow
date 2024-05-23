-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(booking_value_with_is_instant_constraint AS DOUBLE) / CAST(NULLIF(booking_value, 0) AS DOUBLE) AS instant_booking_value_ratio
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_28.metric_time__day) AS metric_time__day
    , MAX(subq_23.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
    , MAX(subq_28.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(booking_value) AS booking_value_with_is_instant_constraint
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['booking_value', 'booking__is_instant', 'metric_time__day']
      SELECT
        metric_time__day
        , booking__is_instant
        , booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , is_instant AS booking__is_instant
          , booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_17
      WHERE booking__is_instant
    ) subq_19
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_23
  FULL OUTER JOIN (
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
  ) subq_28
  ON
    subq_23.metric_time__day = subq_28.metric_time__day
  GROUP BY
    COALESCE(subq_23.metric_time__day, subq_28.metric_time__day)
) subq_29
