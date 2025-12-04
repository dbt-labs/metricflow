test_name: test_min_max_only_categorical
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a categorical dimension.
sql_engine: DuckDB
---
-- Calculate min and max
-- Write to DataTable
SELECT
  MIN(listing__country_latest) AS listing__country_latest__min
  , MAX(listing__country_latest) AS listing__country_latest__max
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['listing__country_latest']
  -- Pass Only Elements: ['listing__country_latest']
  SELECT
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  GROUP BY
    country
) subq_6
