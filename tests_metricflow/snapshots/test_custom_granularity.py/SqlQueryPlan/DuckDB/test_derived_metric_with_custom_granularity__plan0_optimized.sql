-- Compute Metrics via Expressions
SELECT
  booking__ds__martian_day
  , booking_value * 0.05 / bookers AS booking_fees_per_booker
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.booking__ds__martian_day, subq_28.booking__ds__martian_day) AS booking__ds__martian_day
    , MAX(subq_21.booking_value) AS booking_value
    , MAX(subq_28.bookers) AS bookers
  FROM (
    -- Pass Only Elements: ['booking_value', 'booking__ds__day']
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['booking_value', 'booking__ds__martian_day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_17.martian_day AS booking__ds__martian_day
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_17
    ON
      DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_17.ds
    GROUP BY
      subq_17.martian_day
  ) subq_21
  FULL OUTER JOIN (
    -- Pass Only Elements: ['bookers', 'booking__ds__day']
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['bookers', 'booking__ds__martian_day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_24.martian_day AS booking__ds__martian_day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_24
    ON
      DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_24.ds
    GROUP BY
      subq_24.martian_day
  ) subq_28
  ON
    subq_21.booking__ds__martian_day = subq_28.booking__ds__martian_day
  GROUP BY
    COALESCE(subq_21.booking__ds__martian_day, subq_28.booking__ds__martian_day)
) subq_29
