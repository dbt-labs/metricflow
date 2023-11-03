-- Combine Metrics
-- Compute Metrics via Expressions
SELECT
  COALESCE(subq_19.metric_time__day, subq_24.metric_time__day) AS metric_time__day
  , CAST(subq_19.booking_value_with_is_instant_constraint AS DOUBLE) / CAST(NULLIF(subq_24.booking_value, 0) AS DOUBLE) AS instant_booking_value_ratio
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(booking_value) AS booking_value_with_is_instant_constraint
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'booking__is_instant', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , is_instant AS booking__is_instant
      , booking_value
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_15
  WHERE booking__is_instant
  GROUP BY
    metric_time__day
) subq_19
INNER JOIN (
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
) subq_24
ON
  (
    subq_19.metric_time__day = subq_24.metric_time__day
  ) OR (
    (
      subq_19.metric_time__day IS NULL
    ) AND (
      subq_24.metric_time__day IS NULL
    )
  )
