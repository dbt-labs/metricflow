-- Join to Custom Granularity Dataset
-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements: ['booking__ds__martian_day',]
SELECT
  subq_2.martian_day AS booking__ds__martian_day
FROM ***************************.fct_bookings bookings_source_src_28000
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_2
ON
  bookings_source_src_28000.booking__ds__day = subq_2.ds
GROUP BY
  booking__ds__martian_day
