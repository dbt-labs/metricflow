-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'booking__is_instant']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  booking__is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , subq_10.booking__is_instant AS booking__is_instant
    , subq_10.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_10.listing = listings_latest_src_28000.listing_id
) subq_14
WHERE listing__country_latest = 'us'
GROUP BY
  booking__is_instant
