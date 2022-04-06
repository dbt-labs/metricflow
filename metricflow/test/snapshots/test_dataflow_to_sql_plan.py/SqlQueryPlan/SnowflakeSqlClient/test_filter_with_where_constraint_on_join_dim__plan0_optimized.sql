-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'is_instant']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(bookings) AS bookings
  , is_instant
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'is_instant', 'listing__country_latest']
  SELECT
    subq_10.bookings AS bookings
    , subq_10.is_instant AS is_instant
    , listings_latest_src_10003.country AS listing__country_latest
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Elements:
    --   ['bookings', 'is_instant', 'listing']
    SELECT
      1 AS bookings
      , is_instant
      , listing_id AS listing
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    subq_10.listing = listings_latest_src_10003.listing_id
) subq_14
WHERE listing__country_latest = 'us'
GROUP BY
  is_instant
