-- Compute Metrics via Expressions
SELECT
  listing
  , listing__country_latest
  , booking_value * 0.05 AS booking_fees
FROM (
  -- Join Standard Outputs
  -- Aggregate Measures
  SELECT
    bookings_source_src_10001.listing_id AS listing
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(bookings_source_src_10001.booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_10001
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    bookings_source_src_10001.listing_id = listings_latest_src_10004.listing_id
  GROUP BY
    bookings_source_src_10001.listing_id
    , listings_latest_src_10004.country
) subq_11
