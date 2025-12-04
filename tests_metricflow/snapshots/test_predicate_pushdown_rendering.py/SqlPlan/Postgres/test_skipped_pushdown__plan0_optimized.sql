test_name: test_skipped_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect to skip predicate pushdown because it is unsafe.

      This is the query rendering test for the scenarios where the push down evaluation indicates that we should
      skip pushdown, typically due to a lack of certainty over whether or not the query will return the same results.

      The specific scenario is less important here than that it match one that should not be pushed down.
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookings', 'listing__country_latest']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  listing__country_latest
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookings', 'listing__country_latest', 'booking__is_instant', 'listing__is_lux_latest']
  SELECT
    subq_12.booking__is_instant AS booking__is_instant
    , listings_latest_src_28000.country AS listing__country_latest
    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
    , subq_12.__bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
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
WHERE booking__is_instant OR listing__is_lux_latest
GROUP BY
  listing__country_latest
