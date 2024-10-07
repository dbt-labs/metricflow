-- Join Standard Outputs
SELECT
  subq_2.listing AS listing
  , subq_2.bookings AS bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    1 AS bookings
    , listing_id AS listing
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_2
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_2.listing = listings_latest_src_28000.listing_id
