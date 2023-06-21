-- Compute Metrics via Expressions
SELECT
  listing
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(bookers, 0) AS DOUBLE) AS bookings_per_booker
FROM (
  -- Join Standard Outputs
  -- Aggregate Measures
  SELECT
    subq_7.listing AS listing
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_7.bookings) AS bookings
    , COUNT(DISTINCT subq_7.bookers) AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Pass Only Elements:
    --   ['bookings', 'bookers', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS bookings
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_7
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_7.listing = listings_latest_src_10004.listing_id
  GROUP BY
    subq_7.listing
    , listings_latest_src_10004.country
) subq_11
