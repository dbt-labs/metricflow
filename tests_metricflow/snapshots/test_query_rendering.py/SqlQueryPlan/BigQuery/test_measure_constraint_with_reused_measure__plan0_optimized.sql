-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(booking_value_with_is_instant_constraint AS FLOAT64) / CAST(NULLIF(booking_value, 0) AS FLOAT64) AS instant_booking_value_ratio
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_17.metric_time__day, subq_22.metric_time__day) AS metric_time__day
    , MAX(subq_17.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
    , MAX(subq_22.booking_value) AS booking_value
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
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , is_instant AS booking__is_instant
        , booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_13
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_17
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
  ) subq_22
  ON
    subq_17.metric_time__day = subq_22.metric_time__day
  GROUP BY
    metric_time__day
) subq_23
