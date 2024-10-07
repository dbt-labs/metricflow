-- Join Standard Outputs
SELECT
  subq_3.listing AS listing
  , subq_4.country_latest AS listing__country_latest
  , subq_5.country_latest AS listing__country_latest
  , subq_3.bookings AS bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    1 AS bookings
    , listing_id AS listing
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_3
LEFT OUTER JOIN (
  -- Read From SemanticModelDataSet('listings_latest')
  -- Pass Only Elements: ['country_latest', 'listing']
  SELECT
    country AS country_latest
    , listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_4
ON
  subq_3.listing = subq_4.listing
LEFT OUTER JOIN (
  -- Read From SemanticModelDataSet('listings_latest')
  -- Pass Only Elements: ['country_latest', 'listing']
  SELECT
    country AS country_latest
    , listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_5
ON
  subq_3.listing = subq_5.listing
