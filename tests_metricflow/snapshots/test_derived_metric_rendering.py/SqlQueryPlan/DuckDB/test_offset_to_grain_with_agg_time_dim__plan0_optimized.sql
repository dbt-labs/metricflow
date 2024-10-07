-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_15.booking__ds__day, subq_22.booking__ds__day) AS booking__ds__day
    , MAX(subq_15.bookings) AS bookings
    , MAX(subq_22.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__ds__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'booking__ds__day']
      SELECT
        DATE_TRUNC('day', ds) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_13
    GROUP BY
      booking__ds__day
  ) subq_15
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_19.ds AS booking__ds__day
      , SUM(subq_17.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine subq_19
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_17
    ON
      DATE_TRUNC('month', subq_19.ds) = subq_17.booking__ds__day
    GROUP BY
      subq_19.ds
  ) subq_22
  ON
    subq_15.booking__ds__day = subq_22.booking__ds__day
  GROUP BY
    COALESCE(subq_15.booking__ds__day, subq_22.booking__ds__day)
) subq_23
