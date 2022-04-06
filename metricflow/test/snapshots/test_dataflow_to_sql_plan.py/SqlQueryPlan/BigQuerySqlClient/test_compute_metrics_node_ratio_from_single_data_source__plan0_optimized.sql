-- Compute Metrics via Expressions
SELECT
  CAST(bookings AS FLOAT64) / CAST(NULLIF(bookers, 0) AS FLOAT64) AS bookings_per_booker
  , listing__country_latest
  , listing
FROM (
  -- Join Standard Outputs
  -- Aggregate Measures
  SELECT
    SUM(subq_7.bookings) AS bookings
    , COUNT(DISTINCT subq_7.bookers) AS bookers
    , listings_latest_src_10003.country AS listing__country_latest
    , subq_7.listing AS listing
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Elements:
    --   ['bookings', 'bookers', 'listing']
    SELECT
      1 AS bookings
      , guest_id AS bookers
      , listing_id AS listing
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_7
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    subq_7.listing = listings_latest_src_10003.listing_id
  GROUP BY
    listing__country_latest
    , listing
) subq_11
