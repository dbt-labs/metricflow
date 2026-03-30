test_name: test_dimensions_with_filter
test_filename: test_distinct_values_to_sql.py
docstring:
  Tests querying 2 dimensions that require a join.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Select: ['listing__capacity_latest']
-- Write to DataTable
SELECT
  listing__capacity_latest
FROM (
  -- Join Standard Outputs
  -- Select: ['listing__capacity_latest', 'user__home_state_latest']
  SELECT
    listings_latest_src_28000.capacity AS listing__capacity_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    listings_latest_src_28000.user_id = users_latest_src_28000.user_id
) subq_11
WHERE user__home_state_latest = 'us'
GROUP BY
  listing__capacity_latest
