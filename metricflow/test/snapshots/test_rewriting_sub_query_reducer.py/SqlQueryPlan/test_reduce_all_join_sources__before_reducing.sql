-- query
SELECT
  SUM(bookings_src.bookings) AS bookings
  , listings_src1.country_latest AS listing__country_latest
  , listings_src2.capacity_latest AS listing__capacity_latest
  , bookings_src.ds AS ds
FROM (
  -- bookings_src
  SELECT
    fct_bookings_src.booking AS bookings
    , fct_bookings_src.ds
    , fct_bookings_src.listing_id AS listing
  FROM demo.fct_bookings fct_bookings_src
) bookings_src
LEFT OUTER JOIN (
  -- listings_src1
  SELECT
    dim_listings_src1.country AS country_latest
    , dim_listings_src1.listing_id AS listing
  FROM demo.dim_listings dim_listings_src1
) listings_src1
ON
  bookings_src.listing = listings_src1.listing
LEFT OUTER JOIN (
  -- listings_src2
  SELECT
    dim_listings_src2.capacity AS capacity_latest
    , dim_listings_src2.listing_id AS listing
  FROM demo.dim_listings dim_listings_src2
) listings_src2
ON
  listings_src1.listing = listings_src2.listing
GROUP BY
  bookings_src.ds
  , listings_src1.country_latest
  , listings_src2.capacity_latest
