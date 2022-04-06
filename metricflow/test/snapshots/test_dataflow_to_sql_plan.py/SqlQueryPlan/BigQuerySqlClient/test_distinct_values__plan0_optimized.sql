-- Join Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Pass Only Elements:
--   ['listing__country_latest']
-- Order By ['listing__country_latest'] Limit 100
SELECT
  listings_latest_src_10003.country AS listing__country_latest
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_bookings
) bookings_source_src_10000
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_10003
ON
  bookings_source_src_10000.listing_id = listings_latest_src_10003.listing_id
GROUP BY
  listing__country_latest
ORDER BY listing__country_latest
LIMIT 100
