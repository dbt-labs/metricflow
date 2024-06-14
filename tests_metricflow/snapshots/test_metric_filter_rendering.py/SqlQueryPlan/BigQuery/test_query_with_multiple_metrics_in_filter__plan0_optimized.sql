-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'listing__bookings', 'listing__bookers']
  SELECT
    subq_28.listing__bookings AS listing__bookings
    , subq_34.listing__bookers AS listing__bookers
    , subq_22.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'listing', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_22
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_25
    GROUP BY
      listing
  ) subq_28
  ON
    subq_22.listing = subq_28.listing
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'listing']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookers']
    SELECT
      listing_id AS listing
      , COUNT(DISTINCT guest_id) AS listing__bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      listing
  ) subq_34
  ON
    subq_22.listing = subq_34.listing
) subq_36
WHERE listing__bookings > 2 AND listing__bookers > 1
