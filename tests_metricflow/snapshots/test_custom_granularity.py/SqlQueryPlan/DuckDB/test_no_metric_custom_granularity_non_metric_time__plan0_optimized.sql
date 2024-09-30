-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['booking__ds__martian_day',]
SELECT
  subq_1.martian_day AS booking__ds__martian_day
FROM ***************************.fct_bookings bookings_source_src_28000
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_1
ON
  DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_1.ds
GROUP BY
  subq_1.martian_day
