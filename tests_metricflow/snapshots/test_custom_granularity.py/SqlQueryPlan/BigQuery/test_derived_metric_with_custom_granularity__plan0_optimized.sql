-- Compute Metrics via Expressions
SELECT
  booking__ds__martian_day
  , booking_value * 0.05 / bookers AS booking_fees_per_booker
FROM (
  -- Pass Only Elements: ['booking_value', 'bookers', 'booking__ds__day']
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['booking_value', 'bookers', 'booking__ds__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_17.martian_day AS booking__ds__martian_day
    , SUM(bookings_source_src_28000.booking_value) AS booking_value
    , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_17
  ON
    DATETIME_TRUNC(bookings_source_src_28000.ds, day) = subq_17.ds
  GROUP BY
    booking__ds__martian_day
) subq_21
