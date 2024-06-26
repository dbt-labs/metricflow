-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant']
-- Pass Only Elements: ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  listings_latest_src_28000.country AS listing__country_latest
  , SUM(subq_14.bookings) AS bookings
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
  SELECT
    listing
    , bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  WHERE booking__is_instant
) subq_14
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_14.listing = listings_latest_src_28000.listing_id
GROUP BY
  listings_latest_src_28000.country
