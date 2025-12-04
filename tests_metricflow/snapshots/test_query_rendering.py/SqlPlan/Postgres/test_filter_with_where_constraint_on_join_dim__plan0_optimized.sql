test_name: test_filter_with_where_constraint_on_join_dim
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions.
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookings', 'booking__is_instant']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookings', 'booking__is_instant', 'listing__country_latest']
  SELECT
    subq_12.booking__is_instant AS booking__is_instant
    , listings_latest_src_28000.country AS listing__country_latest
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
WHERE listing__country_latest = 'us'
GROUP BY
  booking__is_instant
