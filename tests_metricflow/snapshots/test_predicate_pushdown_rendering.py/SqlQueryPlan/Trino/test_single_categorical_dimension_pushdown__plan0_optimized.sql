-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  listing__country_latest
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant']
  SELECT
    subq_17.booking__is_instant AS booking__is_instant
    , listings_latest_src_28000.country AS listing__country_latest
    , subq_17.bookings AS bookings
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
    SELECT
      listing
      , booking__is_instant
      , bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        listing_id AS listing
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    WHERE booking__is_instant
  ) subq_17
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_17.listing = listings_latest_src_28000.listing_id
) subq_22
WHERE booking__is_instant
GROUP BY
  listing__country_latest
