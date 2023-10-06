-- Combine Metrics
SELECT
  COALESCE(subq_14.metric_time__day, subq_19.metric_time__day) AS metric_time__day
  , MAX(subq_14.bookings) AS bookings
  , MAX(subq_19.booking_value) AS booking_value
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_12
  GROUP BY
    metric_time__day
) subq_14
FULL OUTER JOIN (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC(ds, day) AS metric_time__day
    , SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_10001
  GROUP BY
    metric_time__day
) subq_19
ON
  subq_14.metric_time__day = subq_19.metric_time__day
GROUP BY
  metric_time__day
