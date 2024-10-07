-- Compute Metrics via Expressions
SELECT
  subq_3.listing
  , subq_3.listing__country_latest
  , CAST(subq_3.bookings AS DOUBLE) / CAST(NULLIF(subq_3.bookers, 0) AS DOUBLE) AS bookings_per_booker
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.listing
    , subq_2.listing__country_latest
    , SUM(subq_2.bookings) AS bookings
    , COUNT(DISTINCT subq_2.bookers) AS bookers
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_0.listing AS listing
      , subq_1.country_latest AS listing__country_latest
      , subq_0.bookings AS bookings
      , subq_0.bookers AS bookers
    FROM (
      -- Read From SemanticModelDataSet('bookings_source')
      -- Pass Only Elements: ['bookings', 'bookers', 'listing']
      SELECT
        1 AS bookings
        , bookings_source_src_28000.guest_id AS bookers
        , bookings_source_src_28000.listing_id AS listing
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_0
    LEFT OUTER JOIN (
      -- Read From SemanticModelDataSet('listings_latest')
      -- Pass Only Elements: ['country_latest', 'listing']
      SELECT
        listings_latest_src_28000.country AS country_latest
        , listings_latest_src_28000.listing_id AS listing
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_1
    ON
      subq_0.listing = subq_1.listing
  ) subq_2
  GROUP BY
    subq_2.listing
    , subq_2.listing__country_latest
) subq_3
