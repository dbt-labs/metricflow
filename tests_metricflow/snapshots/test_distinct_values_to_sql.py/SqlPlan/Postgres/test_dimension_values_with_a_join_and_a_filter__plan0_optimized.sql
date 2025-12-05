test_name: test_dimension_values_with_a_join_and_a_filter
test_filename: test_distinct_values_to_sql.py
docstring:
  Tests querying 2 dimensions that require a join and a filter.
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
-- Write to DataTable
SELECT
  listing__is_lux_latest
  , user__home_state_latest
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
  SELECT
    listings_latest_src_28000.is_lux AS listing__is_lux_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    listings_latest_src_28000.user_id = users_latest_src_28000.user_id
) subq_11
WHERE user__home_state_latest = 'us'
GROUP BY
  listing__is_lux_latest
  , user__home_state_latest
