-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_14.metric_time__day, subq_19.metric_time__day) AS metric_time__day
  , MAX(subq_14.bookings) AS bookings
  , MAX(subq_19.listings) AS listings
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
      DATE_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_0
  ) subq_12
  GROUP BY
    metric_time__day
) subq_14
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
      DATE_TRUNC(created_at, day) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_0
  ) subq_17
  GROUP BY
    metric_time__day
) subq_19
ON
  subq_14.metric_time__day = subq_19.metric_time__day
GROUP BY
  metric_time__day
