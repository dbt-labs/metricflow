-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['booking__ds__martian_day',]
SELECT
  subq_0.martian_day AS booking__ds__martian_day
FROM ***************************.fct_bookings bookings_source_src_28000
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_0
ON
  DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_0.ds
GROUP BY
  subq_0.martian_day
