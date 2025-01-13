test_name: test_multi_join_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions.
sql_engine: Clickhouse
---
-- Join Standard Outputs
WITH pfe_1_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['country_latest', 'listing']
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
)

SELECT
  subq_9.country_latest AS listing__country_latest
  , subq_10.country_latest AS listing__country_latest
  , subq_7.listing AS listing
  , subq_7.bookings AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['bookings', 'listing']
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_7
LEFT OUTER JOIN
(
  -- Read From CTE For node_id=pfe_1
  SELECT
    listing
    , country_latest
  FROM pfe_1_cte pfe_1_cte
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_9
ON
  subq_7.listing = subq_9.listing
LEFT OUTER JOIN
(
  -- Read From CTE For node_id=pfe_1
  SELECT
    listing
    , country_latest
  FROM pfe_1_cte pfe_1_cte
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_10
ON
  subq_7.listing = subq_10.listing
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
