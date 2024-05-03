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
    subq_47.listing__views_times_booking_value AS listing__views_times_booking_value
    , subq_34.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_34
  LEFT OUTER JOIN (
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
    SELECT
      listing
      , booking_value * views AS listing__views_times_booking_value
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        COALESCE(subq_39.listing, subq_44.listing) AS listing
        , MAX(subq_39.booking_value) AS booking_value
        , MAX(subq_44.views) AS views
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
      ) subq_39
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
        ) subq_42
        GROUP BY
          listing
      ) subq_44
      ON
        subq_39.listing = subq_44.listing
      GROUP BY
        COALESCE(subq_39.listing, subq_44.listing)
    ) subq_45
  ) subq_47
  ON
    subq_34.listing = subq_47.listing
) subq_49
WHERE listing__views_times_booking_value > 1
