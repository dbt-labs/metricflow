test_name: test_no_dedupe_saved_query
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__capacity_latest', 'metric_time__month']
-- Write to DataTable
SELECT
  metric_time__month
  , listing__capacity_latest
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listing__capacity_latest', 'user__home_state_latest', 'metric_time__month']
  SELECT
    DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
    , listings_latest_src_28000.capacity AS listing__capacity_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  CROSS JOIN
    ***************************.mf_time_spine time_spine_src_28006
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    listings_latest_src_28000.user_id = users_latest_src_28000.user_id
) subq_17
WHERE user__home_state_latest = 'CA'
