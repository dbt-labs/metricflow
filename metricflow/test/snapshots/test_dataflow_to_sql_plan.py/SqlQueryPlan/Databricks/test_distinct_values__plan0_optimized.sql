-- Join Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Pass Only Elements:
--   ['listing__country_latest']
-- Order By ['listing__country_latest'] Limit 100
SELECT
  listings_latest_src_10004.country AS listing__country_latest
FROM ***************************.fct_bookings bookings_source_src_10001
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_10004
ON
  bookings_source_src_10001.listing_id = listings_latest_src_10004.listing_id
GROUP BY
  listings_latest_src_10004.country
ORDER BY listing__country_latest
LIMIT 100
