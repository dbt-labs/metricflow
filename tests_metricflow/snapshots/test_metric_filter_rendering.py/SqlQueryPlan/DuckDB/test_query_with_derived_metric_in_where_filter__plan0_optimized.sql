-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'listing__views_times_booking_value']
  SELECT
    subq_26.listing__views_times_booking_value AS listing__views_times_booking_value
    , subq_16.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_16
  LEFT OUTER JOIN (
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
    SELECT
      listing
      , booking_value * views AS listing__views_times_booking_value
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        COALESCE(subq_20.listing, subq_24.listing) AS listing
        , MAX(subq_20.booking_value) AS booking_value
        , MAX(subq_24.views) AS views
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['booking_value', 'listing']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          listing_id AS listing
          , SUM(booking_value) AS booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
        GROUP BY
          listing_id
      ) subq_20
      FULL OUTER JOIN (
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          listing
          , SUM(views) AS views
        FROM (
          -- Read Elements From Semantic Model 'views_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['views', 'listing']
          SELECT
            listing_id AS listing
            , 1 AS views
          FROM ***************************.fct_views views_source_src_28000
        ) subq_22
        GROUP BY
          listing
      ) subq_24
      ON
        subq_20.listing = subq_24.listing
      GROUP BY
        COALESCE(subq_20.listing, subq_24.listing)
    ) subq_25
  ) subq_26
  ON
    subq_16.listing = subq_26.listing
) subq_27
WHERE listing__views_times_booking_value > 1
