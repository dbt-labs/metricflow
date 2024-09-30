-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'booking__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_5.martian_day AS booking__ds__martian_day
  , SUM(subq_4.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS bookings
    , DATE_TRUNC('day', ds) AS booking__ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_4
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_5
ON
  subq_4.booking__ds__day = subq_5.ds
GROUP BY
  subq_5.martian_day
