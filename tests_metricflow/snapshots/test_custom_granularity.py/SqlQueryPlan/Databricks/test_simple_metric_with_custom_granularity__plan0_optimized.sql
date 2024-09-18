-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'booking__ds__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_7.martian_day AS booking__ds__martian_day
  , SUM(subq_6.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS booking__ds__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_6
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_7
ON
  subq_6.booking__ds__day = subq_7.ds
GROUP BY
  subq_7.martian_day
