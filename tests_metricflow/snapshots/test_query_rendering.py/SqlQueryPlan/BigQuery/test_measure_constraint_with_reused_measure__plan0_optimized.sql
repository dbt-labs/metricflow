-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(booking_value_with_is_instant_constraint AS FLOAT64) / CAST(NULLIF(booking_value, 0) AS FLOAT64) AS instant_booking_value_ratio
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__day, subq_24.metric_time__day) AS metric_time__day
    , MAX(subq_19.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
    , MAX(subq_24.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(booking_value) AS booking_value_with_is_instant_constraint
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['booking_value', 'booking__is_instant', 'metric_time__day']
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , is_instant AS booking__is_instant
        , booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_19
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      metric_time__day
  ) subq_24
  ON
    subq_19.metric_time__day = subq_24.metric_time__day
  GROUP BY
    metric_time__day
) subq_25
