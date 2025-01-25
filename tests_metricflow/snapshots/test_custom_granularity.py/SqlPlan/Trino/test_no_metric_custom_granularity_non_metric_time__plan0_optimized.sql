test_name: test_no_metric_custom_granularity_non_metric_time
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Read Elements From Semantic Model 'bookings_source'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['booking__ds__martian_day',]
SELECT
  nr_subq_2.martian_day AS booking__ds__martian_day
FROM ***************************.fct_bookings bookings_source_src_28000
LEFT OUTER JOIN
  ***************************.mf_time_spine nr_subq_2
ON
  DATE_TRUNC('day', bookings_source_src_28000.ds) = nr_subq_2.ds
GROUP BY
  nr_subq_2.martian_day
