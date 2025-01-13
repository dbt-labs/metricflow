test_name: test_distinct_values
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get distinct values for a dimension.
sql_engine: Clickhouse
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__country_latest',]
-- Order By ['listing__country_latest'] Limit 100
SELECT
  listing__country_latest
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_3
WHERE listing__country_latest = 'us'
GROUP BY
  listing__country_latest
ORDER BY listing__country_latest DESC
LIMIT 100
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
