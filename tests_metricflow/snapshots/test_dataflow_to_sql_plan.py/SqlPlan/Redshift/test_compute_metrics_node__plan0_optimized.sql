test_name: test_compute_metrics_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node.
sql_engine: Redshift
---
-- Join Standard Outputs
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_1.listing AS listing
  , listings_latest_src_28000.country AS listing__country_latest
  , SUM(nr_subq_1.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) nr_subq_1
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  nr_subq_1.listing = listings_latest_src_28000.listing_id
GROUP BY
  nr_subq_1.listing
  , listings_latest_src_28000.country
