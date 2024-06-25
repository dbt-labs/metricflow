-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'listing__bookings_per_booker']
  SELECT
    CAST(subq_56.bookings AS DOUBLE) / CAST(NULLIF(subq_56.bookers, 0) AS DOUBLE) AS listing__bookings_per_booker
    , subq_45.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_45
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_50.listing, subq_55.listing) AS listing
      , MAX(subq_50.bookings) AS bookings
      , MAX(subq_55.bookers) AS bookers
    FROM (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
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
      ) subq_48
      GROUP BY
        listing
    ) subq_50
    FULL OUTER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookers', 'listing']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        listing_id AS listing
        , COUNT(DISTINCT guest_id) AS bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
      GROUP BY
        listing_id
    ) subq_55
    ON
      subq_50.listing = subq_55.listing
    GROUP BY
      COALESCE(subq_50.listing, subq_55.listing)
  ) subq_56
  ON
    subq_45.listing = subq_56.listing
) subq_60
WHERE listing__bookings_per_booker > 1
