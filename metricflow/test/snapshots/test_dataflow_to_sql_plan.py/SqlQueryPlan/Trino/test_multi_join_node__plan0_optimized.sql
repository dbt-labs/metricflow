-- Join Standard Outputs
SELECT
  subq_7.listing AS listing
  , subq_9.country_latest AS listing__country_latest
  , subq_11.country_latest AS listing__country_latest
  , subq_7.bookings AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_7
LEFT OUTER JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements:
  --   ['country_latest', 'listing']
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10004
) subq_9
ON
  subq_7.listing = subq_9.listing
LEFT OUTER JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements:
  --   ['country_latest', 'listing']
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10004
) subq_11
ON
  subq_7.listing = subq_11.listing
