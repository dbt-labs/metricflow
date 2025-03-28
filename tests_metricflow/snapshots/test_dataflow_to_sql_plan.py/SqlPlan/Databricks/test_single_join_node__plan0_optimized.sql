test_name: test_single_join_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 1 dimension.
sql_engine: Databricks
---
-- Join Standard Outputs
SELECT
  subq_5.listing AS listing
  , subq_5.bookings AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_5
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_5.listing = listings_latest_src_28000.listing_id
