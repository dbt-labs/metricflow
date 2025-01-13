test_name: test_no_metric_custom_granularity_non_metric_time
test_filename: test_custom_granularity.py
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'bookings_source'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['booking__ds__martian_day',]
SELECT
  subq_2.martian_day AS booking__ds__martian_day
FROM ***************************.fct_bookings bookings_source_src_28000
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_2
ON
  DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_2.ds
GROUP BY
  subq_2.martian_day
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
