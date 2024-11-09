test_name: test_single_categorical_dimension_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect predicate pushdown for a single categorical dimension.
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  listing__country_latest
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
WHERE booking__is_instant
GROUP BY
  listing__country_latest
