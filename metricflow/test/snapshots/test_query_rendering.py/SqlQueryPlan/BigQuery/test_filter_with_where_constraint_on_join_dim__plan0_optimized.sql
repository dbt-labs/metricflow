-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'booking__is_instant']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  booking__is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'booking__is_instant', 'listing__country_latest']
  SELECT
    subq_13.booking__is_instant AS booking__is_instant
    , listings_latest_src_10005.country AS listing__country_latest
    , subq_13.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'booking__is_instant', 'listing']
    SELECT
      listing_id AS listing
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_13
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10005
  ON
    subq_13.listing = listings_latest_src_10005.listing_id
) subq_18
WHERE listing__country_latest = 'us'
GROUP BY
  booking__is_instant
