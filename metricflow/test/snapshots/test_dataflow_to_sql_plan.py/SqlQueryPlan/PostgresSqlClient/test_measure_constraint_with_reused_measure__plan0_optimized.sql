-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['metric_time', 'booking_value_with_is_instant_constraint', 'booking_value']
-- Compute Metrics via Expressions
SELECT
  subq_20.metric_time AS metric_time
  , CAST(subq_20.booking_value_with_is_instant_constraint AS DOUBLE PRECISION) / CAST(NULLIF(subq_25.booking_value, 0) AS DOUBLE PRECISION) AS instant_booking_value_ratio
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  SELECT
    metric_time
    , SUM(booking_value) AS booking_value_with_is_instant_constraint
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Additive Measures
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'is_instant', 'metric_time']
    SELECT
      ds AS metric_time
      , is_instant
      , booking_value
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
  ) subq_17
  WHERE is_instant
  GROUP BY
    metric_time
) subq_20
INNER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Pass Only Additive Measures
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  SELECT
    ds AS metric_time
    , SUM(booking_value) AS booking_value
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10001
  GROUP BY
    ds
) subq_25
ON
  (
    (
      subq_20.metric_time = subq_25.metric_time
    ) OR (
      (subq_20.metric_time IS NULL) AND (subq_25.metric_time IS NULL)
    )
  )
