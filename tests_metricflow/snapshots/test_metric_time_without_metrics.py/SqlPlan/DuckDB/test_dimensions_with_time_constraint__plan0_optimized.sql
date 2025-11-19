test_name: test_dimensions_with_time_constraint
test_filename: test_metric_time_without_metrics.py
sql_engine: DuckDB
---
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-03T00:00:00]
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
-- Write to DataTable
SELECT
  metric_time__day
  , listing__is_lux_latest
  , user__home_state_latest
FROM (
  -- Join Standard Outputs
  SELECT
    users_latest_src_28000.home_state_latest AS user__home_state_latest
    , time_spine_src_28006.ds AS metric_time__day
    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  CROSS JOIN
    ***************************.mf_time_spine time_spine_src_28006
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    listings_latest_src_28000.user_id = users_latest_src_28000.user_id
) subq_15
WHERE metric_time__day BETWEEN '2020-01-01' AND '2020-01-03'
GROUP BY
  metric_time__day
  , listing__is_lux_latest
  , user__home_state_latest
