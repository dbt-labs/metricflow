test_name: test_dimension_with_joined_where_constraint
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests querying 2 dimensions that require a join.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['user__home_state_latest']
-- Write to DataTable
SELECT
  user__home_state_latest
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__home_state_latest', 'listing__country_latest']
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    listings_latest_src_28000.user_id = users_latest_src_28000.user_id
) subq_11
WHERE listing__country_latest = 'us'
GROUP BY
  user__home_state_latest
