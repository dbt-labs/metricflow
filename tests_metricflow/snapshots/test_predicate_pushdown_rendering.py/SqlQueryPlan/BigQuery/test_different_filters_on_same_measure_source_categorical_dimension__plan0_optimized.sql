-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(average_booking_value AS FLOAT64) / CAST(NULLIF(max_booking_value, 0) AS FLOAT64) AS instant_booking_fraction_of_max_value
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__day, subq_24.metric_time__day) AS metric_time__day
    , MAX(subq_19.average_booking_value) AS average_booking_value
    , MAX(subq_24.max_booking_value) AS max_booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['average_booking_value', 'booking__is_instant', 'metric_time__day']
    -- Pass Only Elements: ['average_booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , is_instant AS booking__is_instant
        , booking_value AS average_booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_14
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_19
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['max_booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , MAX(booking_value) AS max_booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      metric_time__day
  ) subq_24
  ON
    subq_19.metric_time__day = subq_24.metric_time__day
  GROUP BY
    metric_time__day
) subq_25
