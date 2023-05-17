-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'is_instant']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'is_instant', 'listing__country_latest']
  SELECT
    subq_13.is_instant AS is_instant
    , listings_latest_src_10004.country AS listing__country_latest
    , subq_13.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'is_instant', 'listing']
    SELECT
      listing_id AS listing
      , is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_13
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_13.listing = listings_latest_src_10004.listing_id
) subq_18
WHERE listing__country_latest = 'us'
GROUP BY
  is_instant
