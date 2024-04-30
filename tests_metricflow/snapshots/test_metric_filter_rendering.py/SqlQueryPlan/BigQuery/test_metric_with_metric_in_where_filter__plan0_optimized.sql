-- Constrain Output with WHERE
-- Pass Only Elements: ['listings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(listings) AS active_listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'metric_time__day', 'listing__bookings']
  SELECT
    subq_20.metric_time__day AS metric_time__day
    , subq_26.bookings AS listing__bookings
    , subq_20.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC(created_at, day) AS metric_time__day
      , listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_20
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_23
    GROUP BY
      listing
  ) subq_26
  ON
    subq_20.listing = subq_26.listing
) subq_28
WHERE listing__bookings > 2
GROUP BY
  metric_time__day
