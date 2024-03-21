-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_24.metric_time__day, subq_29.metric_time__day) AS metric_time__day
  , MAX(subq_24.bookings) AS bookings
  , MAX(subq_29.listings) AS listings
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_0
  ) subq_22
  GROUP BY
    metric_time__day
) subq_24
FULL OUTER JOIN (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', created_at) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_0
  ) subq_27
  GROUP BY
    metric_time__day
) subq_29
ON
  subq_24.metric_time__day = subq_29.metric_time__day
GROUP BY
  COALESCE(subq_24.metric_time__day, subq_29.metric_time__day)
