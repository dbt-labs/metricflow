test_name: test_local_dimension_using_local_entity
test_filename: test_query_rendering.py
sql_engine: Trino
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  listing__country_latest
  , SUM(listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['listings', 'listing__country_latest']
  SELECT
    country AS listing__country_latest
    , 1 AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_7
GROUP BY
  listing__country_latest
