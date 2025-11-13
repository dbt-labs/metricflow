test_name: test_compute_metrics_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node.
sql_engine: DuckDB
---
-- Join Standard Outputs
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
SELECT
  subq_7.listing AS listing
  , listings_latest_src_28000.country AS listing__country_latest
  , SUM(subq_7.__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['__bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_7.listing = listings_latest_src_28000.listing_id
GROUP BY
  subq_7.listing
  , listings_latest_src_28000.country
