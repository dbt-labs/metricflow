test_name: test_metric_time_with_other_dimensions
test_filename: test_metric_time_without_metrics.py
sql_engine: Redshift
---
-- Join Standard Outputs
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
-- Write to DataTable
SELECT
  time_spine_src_28006.ds AS metric_time__day
  , listings_latest_src_28000.is_lux AS listing__is_lux_latest
  , users_latest_src_28000.home_state_latest AS user__home_state_latest
FROM ***************************.dim_listings_latest listings_latest_src_28000
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_28006
FULL OUTER JOIN
  ***************************.dim_users_latest users_latest_src_28000
ON
  listings_latest_src_28000.user_id = users_latest_src_28000.user_id
GROUP BY
  time_spine_src_28006.ds
  , listings_latest_src_28000.is_lux
  , users_latest_src_28000.home_state_latest
