-- Compute Metrics via Expressions
SELECT
  listing
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(bookers, 0) AS DOUBLE) AS bookings_per_booker
FROM (
  -- Join Standard Outputs
  -- Aggregate Measures
  SELECT
    subq_4.listing AS listing
    , listings_latest_src_28000.country AS listing__country_latest
    , SUM(subq_4.bookings) AS bookings
    , COUNT(DISTINCT subq_4.bookers) AS bookers
  FROM (
    -- Read From SemanticModelDataSet('bookings_source')
    -- Pass Only Elements: ['bookings', 'bookers', 'listing']
    SELECT
      1 AS bookings
      , guest_id AS bookers
      , listing_id AS listing
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_4
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_4.listing = listings_latest_src_28000.listing_id
  GROUP BY
    subq_4.listing
    , listings_latest_src_28000.country
) subq_7
