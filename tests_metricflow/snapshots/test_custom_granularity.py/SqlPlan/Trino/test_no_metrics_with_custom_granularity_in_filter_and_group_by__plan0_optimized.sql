test_name: test_no_metrics_with_custom_granularity_in_filter_and_group_by
test_filename: test_custom_granularity.py
docstring:
  Group by items only queried with a filter on a custom grain, where that grain is also used in the group by.
sql_engine: Trino
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__ds__martian_day',]
SELECT
  listing__ds__martian_day
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Join to Custom Granularity Dataset
  SELECT
    nr_subq_3.martian_day AS listing__ds__martian_day
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_3
  ON
    DATE_TRUNC('day', listings_latest_src_28000.created_at) = nr_subq_3.ds
) nr_subq_4
WHERE listing__ds__martian_day = '2020-01-01'
GROUP BY
  listing__ds__martian_day
