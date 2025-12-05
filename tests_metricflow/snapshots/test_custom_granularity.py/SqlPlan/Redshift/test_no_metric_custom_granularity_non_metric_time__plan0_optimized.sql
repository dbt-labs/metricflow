test_name: test_no_metric_custom_granularity_non_metric_time
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Read Elements From Semantic Model 'bookings_source'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['booking__ds__alien_day']
-- Pass Only Elements: ['booking__ds__alien_day']
-- Write to DataTable
SELECT
  subq_4.alien_day AS booking__ds__alien_day
FROM ***************************.fct_bookings bookings_source_src_28000
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_4
ON
  DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_4.ds
GROUP BY
  subq_4.alien_day
