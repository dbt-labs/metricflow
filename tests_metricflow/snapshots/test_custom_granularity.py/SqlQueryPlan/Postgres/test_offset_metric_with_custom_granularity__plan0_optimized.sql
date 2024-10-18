-- Compute Metrics via Expressions
SELECT
  booking__ds__martian_day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Pass Only Elements: ['bookings', 'booking__ds__day']
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'booking__ds__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_15.martian_day AS booking__ds__martian_day
    , SUM(subq_14.bookings) AS bookings_5_days_ago
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_13.ds AS booking__ds__day
      , subq_11.bookings AS bookings
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    ON
      subq_13.ds - MAKE_INTERVAL(days => 5) = subq_11.booking__ds__day
  ) subq_14
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_15
  ON
    subq_14.booking__ds__day = subq_15.ds
  GROUP BY
    subq_15.martian_day
) subq_19
