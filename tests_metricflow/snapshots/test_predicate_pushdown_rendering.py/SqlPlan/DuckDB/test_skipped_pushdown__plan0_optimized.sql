test_name: test_skipped_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect to skip predicate pushdown because it is unsafe.

      This is the query rendering test for the scenarios where the push down evaluation indicates that we should
      skip pushdown, typically due to a lack of certainty over whether or not the query will return the same results.

      The specific scenario is less important here than that it match one that should not be pushed down.
sql_engine: DuckDB
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
    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
    , nr_subq_7.booking__is_instant AS booking__is_instant
    , nr_subq_7.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_7
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    nr_subq_7.listing = listings_latest_src_28000.listing_id
) nr_subq_10
WHERE booking__is_instant OR listing__is_lux_latest
GROUP BY
  listing__country_latest
