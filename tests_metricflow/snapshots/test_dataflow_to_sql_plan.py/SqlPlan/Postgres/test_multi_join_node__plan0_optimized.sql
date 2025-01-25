test_name: test_multi_join_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions.
sql_engine: Postgres
---
-- Join Standard Outputs
SELECT
  subq_4.country_latest AS listing__country_latest
  , subq_5.country_latest AS listing__country_latest
  , subq_3.listing AS listing
  , subq_3.bookings AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_3
LEFT OUTER JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['country_latest', 'listing']
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_4
ON
  subq_3.listing = subq_4.listing
LEFT OUTER JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['country_latest', 'listing']
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_5
ON
  subq_3.listing = subq_5.listing
