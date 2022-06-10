-- Join Standard Outputs
SELECT
  subq_5.bookings AS bookings
  , listings_latest_src_10003.country AS listing__country_latest
  , subq_5.listing AS listing
FROM (
  -- Read Elements From Data Source 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'listing']
  SELECT
    1 AS bookings
    , listing_id AS listing
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
) subq_5
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_10003
ON
  subq_5.listing = listings_latest_src_10003.listing_id
