test_name: test_aliases_without_metrics
test_filename: test_query_rendering.py
docstring:
  Tests a plan with an aliased dimension.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__capacity_latest', 'listing']
-- Order By ['listing__capacity_latest', 'listing']
-- Change Column Aliases
-- Write to DataTable
SELECT
  listing AS listing_id
  , listing__capacity_latest AS listing_capacity
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['listing__capacity_latest', 'listing']
  SELECT
    listing_id AS listing
    , capacity AS listing__capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_7
WHERE listing__capacity_latest > 2
GROUP BY
  listing
  , listing__capacity_latest
ORDER BY listing_capacity, listing_id
