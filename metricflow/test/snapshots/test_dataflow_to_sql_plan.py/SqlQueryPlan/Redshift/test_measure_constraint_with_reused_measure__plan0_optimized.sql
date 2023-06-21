-- Combine Metrics
-- Compute Metrics via Expressions
SELECT
  COALESCE(subq_19.metric_time, subq_24.metric_time) AS metric_time
  , CAST(subq_19.booking_value_with_is_instant_constraint AS DOUBLE PRECISION) / CAST(NULLIF(subq_24.booking_value, 0) AS DOUBLE PRECISION) AS instant_booking_value_ratio
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time
    , SUM(booking_value) AS booking_value_with_is_instant_constraint
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'is_instant', 'metric_time']
    SELECT
      ds AS metric_time
      , is_instant
      , booking_value
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_15
  WHERE is_instant
  GROUP BY
    metric_time
) subq_19
INNER JOIN (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    ds AS metric_time
    , SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_10001
  GROUP BY
    ds
) subq_24
ON
  (
    subq_19.metric_time = subq_24.metric_time
  ) OR (
    (subq_19.metric_time IS NULL) AND (subq_24.metric_time IS NULL)
  )
