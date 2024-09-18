-- Pass Only Elements: ['bookings', 'booking__ds__day']
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'booking__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_8.martian_day AS booking__ds__martian_day
  , SUM(subq_7.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS booking__ds__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_8
ON
  subq_7.booking__ds__day = subq_8.ds
GROUP BY
  booking__ds__martian_day
