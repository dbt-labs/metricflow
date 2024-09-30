-- Compute Metrics via Expressions
SELECT
  subq_3.listing
  , subq_3.listing__country_latest
  , booking_value * 0.05 AS booking_fees
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.listing
    , subq_2.listing__country_latest
    , SUM(subq_2.booking_value) AS booking_value
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_0.listing AS listing
      , subq_1.country_latest AS listing__country_latest
      , subq_0.booking_value AS booking_value
    FROM (
      -- Read From SemanticModelDataSet('bookings_source')
      -- Pass Only Elements: ['booking_value', 'listing']
      SELECT
        bookings_source_src_28000.booking_value
        , bookings_source_src_28000.listing_id AS listing
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_0
    LEFT OUTER JOIN (
      -- Read From SemanticModelDataSet('listings_latest')
      -- Pass Only Elements: ['country_latest', 'listing']
      SELECT
        listings_latest_src_28000.country AS country_latest
        , listings_latest_src_28000.listing_id AS listing
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_1
    ON
      subq_0.listing = subq_1.listing
  ) subq_2
  GROUP BY
    subq_2.listing
    , subq_2.listing__country_latest
) subq_3
