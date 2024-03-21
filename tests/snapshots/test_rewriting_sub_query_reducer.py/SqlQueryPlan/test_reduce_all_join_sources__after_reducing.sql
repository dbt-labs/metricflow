-- query
SELECT
  SUM(fct_bookings_src.booking) AS bookings
  , dim_listings_src1.country AS listing__country_latest
  , dim_listings_src2.capacity AS listing__capacity_latest
  , fct_bookings_src.ds AS ds
FROM demo.fct_bookings fct_bookings_src
LEFT OUTER JOIN
  demo.dim_listings dim_listings_src1
ON
  fct_bookings_src.listing_id = dim_listings_src1.listing_id
LEFT OUTER JOIN
  demo.dim_listings dim_listings_src2
ON
  dim_listings_src1.listing_id = dim_listings_src2.listing_id
GROUP BY
  fct_bookings_src.ds
  , dim_listings_src1.country
  , dim_listings_src2.capacity
