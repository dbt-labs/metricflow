-- Join Standard Outputs
SELECT
  subq_5.listing AS listing
  , subq_5.bookings AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_5
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_10004
ON
  subq_5.listing = listings_latest_src_10004.listing_id
