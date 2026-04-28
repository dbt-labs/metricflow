test_name: test_single_categorical_dimension_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect predicate pushdown for a single categorical dimension.
sql_engine: ClickHouse
---
SELECT
  listing__country_latest
  , SUM(__bookings) AS bookings
FROM (
  SELECT
    listing__country_latest
    , bookings AS __bookings
  FROM (
    SELECT
      subq_12.booking__is_instant AS booking__is_instant
      , listings_latest_src_28000.country AS listing__country_latest
      , subq_12.__bookings AS bookings
    FROM (
      SELECT
        listing_id AS listing
        , is_instant AS booking__is_instant
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_12.listing = listings_latest_src_28000.listing_id
  ) subq_17
  WHERE booking__is_instant
) subq_19
GROUP BY
  listing__country_latest
