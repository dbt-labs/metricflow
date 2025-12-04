test_name: test_dimensions_requiring_join
test_filename: test_distinct_values_to_sql.py
docstring:
  Tests querying 2 dimensions that require a join.
sql_engine: Snowflake
---
-- Join Standard Outputs
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
-- Write to DataTable
SELECT
  listings_latest_src_28000.is_lux AS listing__is_lux_latest
  , users_latest_src_28000.home_state_latest AS user__home_state_latest
FROM ***************************.dim_listings_latest listings_latest_src_28000
FULL OUTER JOIN
  ***************************.dim_users_latest users_latest_src_28000
ON
  listings_latest_src_28000.user_id = users_latest_src_28000.user_id
GROUP BY
  listings_latest_src_28000.is_lux
  , users_latest_src_28000.home_state_latest
