-- Join Standard Outputs
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(subq_7.bookings) AS bookings
  , listings_latest_src_10003.country AS listing__country_latest
  , subq_7.listing AS listing
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
) subq_7
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_10003
ON
  subq_7.listing = listings_latest_src_10003.listing_id
GROUP BY
  listings_latest_src_10003.country
  , subq_7.listing
