-- Combine Metrics
SELECT
  COALESCE(subq_14.metric_time, subq_19.metric_time) AS metric_time
  , MAX(subq_14.bookings) AS bookings
  , MAX(subq_19.booking_value) AS booking_value
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'metric_time']
    SELECT
      ds AS metric_time
      , 1 AS bookings
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
  ) subq_12
  GROUP BY
    metric_time
) subq_14
FULL OUTER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    ds AS metric_time
    , SUM(booking_value) AS booking_value
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10001
  GROUP BY
    ds
) subq_19
ON
  subq_14.metric_time = subq_19.metric_time
GROUP BY
  COALESCE(subq_14.metric_time, subq_19.metric_time)
