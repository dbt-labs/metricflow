test_name: test_compute_metrics_node_ratio_from_single_semantic_model
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the compute metrics node for ratio type metrics sourced from a single semantic model.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  listing
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(bookers, 0) AS DOUBLE) AS bookings_per_booker
FROM (
  -- Join Standard Outputs
  -- Aggregate Inputs for Simple Metrics
  SELECT
    bookings_source_src_28000.listing_id AS listing
    , listings_latest_src_28000.country AS listing__country_latest
  FROM ***************************.fct_bookings bookings_source_src_28000
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    bookings_source_src_28000.listing_id = listings_latest_src_28000.listing_id
  GROUP BY
    bookings_source_src_28000.listing_id
    , listings_latest_src_28000.country
) subq_11
