test_name: test_local_dimension_using_local_entity
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  listing__country_latest
  , SUM(__listings) AS listings
FROM (
  SELECT
    country AS listing__country_latest
    , 1 AS __listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_9
GROUP BY
  listing__country_latest
