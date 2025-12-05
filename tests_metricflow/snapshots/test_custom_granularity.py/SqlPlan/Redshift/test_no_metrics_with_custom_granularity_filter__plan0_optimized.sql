test_name: test_no_metrics_with_custom_granularity_filter
test_filename: test_custom_granularity.py
docstring:
  Group by items only queried with a filter on a custom grain, where that grain is not used in the group by.
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__ds__day']
-- Write to DataTable
SELECT
  listing__ds__day
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['listing__ds__day', 'listing__ds__alien_day']
  SELECT
    subq_5.alien_day AS listing__ds__alien_day
    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_5
  ON
    DATE_TRUNC('day', listings_latest_src_28000.created_at) = subq_5.ds
) subq_7
WHERE listing__ds__alien_day = '2020-01-01'
GROUP BY
  listing__ds__day
