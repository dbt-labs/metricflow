-- query
SELECT
  SUM(bookings_src.bookings) AS bookings
  , listings_src.listing__country_latest AS listing__country_latest
  , bookings_src.ds AS ds
FROM (
  -- bookings_src
  SELECT
    colliding_alias.booking AS bookings
    , colliding_alias.ds
    , colliding_alias.listing_id AS listing
  FROM demo.fct_bookings colliding_alias
) bookings_src
LEFT OUTER JOIN (
  -- listings_src
  SELECT
    colliding_alias.country AS country_latest
    , colliding_alias.listing_id AS listing
  FROM demo.dim_listings colliding_alias
) listings_src
ON
  bookings_src.listing = listings_src.listing
WHERE bookings_src.ds <= '2020-01-05'
GROUP BY
  bookings_src.ds
