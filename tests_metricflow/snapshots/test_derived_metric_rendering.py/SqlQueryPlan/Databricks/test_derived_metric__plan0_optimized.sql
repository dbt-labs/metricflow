-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , (bookings - ref_bookings) * 1.0 / bookings AS non_referred_bookings_pct
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(referred_bookings) AS ref_bookings
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['referred_bookings', 'bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
      , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  GROUP BY
    metric_time__day
) subq_15
