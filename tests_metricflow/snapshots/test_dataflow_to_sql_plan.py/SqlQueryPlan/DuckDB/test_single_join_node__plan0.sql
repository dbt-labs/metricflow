-- Join Standard Outputs
SELECT
  subq_0.listing AS listing
  , subq_0.bookings AS bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    1 AS bookings
    , bookings_source_src_28000.listing_id AS listing
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_0
LEFT OUTER JOIN (
  -- Read From SemanticModelDataSet('listings_latest')
  -- Pass Only Elements: ['listing__country_latest', 'listing']
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , listings_latest_src_28000.listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_1
ON
  subq_0.listing = subq_1.listing
