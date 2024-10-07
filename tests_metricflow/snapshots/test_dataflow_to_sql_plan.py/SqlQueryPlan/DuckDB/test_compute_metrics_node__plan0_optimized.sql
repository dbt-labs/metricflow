-- Join Standard Outputs
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_4.listing AS listing
  , listings_latest_src_28000.country AS listing__country_latest
  , SUM(subq_4.bookings) AS bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    1 AS bookings
    , listing_id AS listing
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_4
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_4.listing = listings_latest_src_28000.listing_id
GROUP BY
  subq_4.listing
  , listings_latest_src_28000.country
