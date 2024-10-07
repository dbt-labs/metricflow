-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.booking__ds__day, subq_22.booking__ds__day) AS booking__ds__day
    , MAX(subq_18.booking_value) AS booking_value
    , MAX(subq_22.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_15.ds AS booking__ds__day
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      subq_15.ds - INTERVAL 1 week = DATE_TRUNC('day', bookings_source_src_28000.ds)
    GROUP BY
      subq_15.ds
  ) subq_18
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , COUNT(DISTINCT guest_id) AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      DATE_TRUNC('day', ds)
  ) subq_22
  ON
    subq_18.booking__ds__day = subq_22.booking__ds__day
  GROUP BY
    COALESCE(subq_18.booking__ds__day, subq_22.booking__ds__day)
) subq_23
