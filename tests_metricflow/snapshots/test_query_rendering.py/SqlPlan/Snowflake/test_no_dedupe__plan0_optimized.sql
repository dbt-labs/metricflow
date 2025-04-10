test_name: test_no_dedupe
test_filename: test_query_rendering.py
sql_engine: Snowflake
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__capacity', 'metric_time__month']
SELECT
  metric_time__month
  , listing__capacity
FROM (
  -- Join Standard Outputs
  SELECT
    users_latest_src_26000.home_state_latest AS user__home_state_latest
    , DATE_TRUNC('month', time_spine_src_26006.ds) AS metric_time__month
    , listings_src_26000.capacity AS listing__capacity
  FROM ***************************.dim_listings listings_src_26000
  CROSS JOIN
    ***************************.mf_time_spine time_spine_src_26006
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_26000
  ON
    listings_src_26000.user_id = users_latest_src_26000.user_id
) subq_14
WHERE user__home_state_latest = 'CA'
