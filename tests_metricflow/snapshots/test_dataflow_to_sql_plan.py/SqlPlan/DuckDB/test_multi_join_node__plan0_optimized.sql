test_name: test_multi_join_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions.
sql_engine: DuckDB
---
-- Join Standard Outputs
WITH pfe_1_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['country_latest', 'listing']
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  subq_8.country_latest AS listing__country_latest
  , subq_9.country_latest AS listing__country_latest
  , subq_6.listing AS listing
  , subq_6.__bookings AS __bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['__bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_6
LEFT OUTER JOIN (
  -- Read From CTE For node_id=pfe_1
  SELECT
    listing
    , country_latest
  FROM pfe_1_cte
) subq_8
ON
  subq_6.listing = subq_8.listing
LEFT OUTER JOIN (
  -- Read From CTE For node_id=pfe_1
  SELECT
    listing
    , country_latest
  FROM pfe_1_cte
) subq_9
ON
  subq_6.listing = subq_9.listing
