test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.booking__ds__day, subq_26.booking__ds__day) AS booking__ds__day
    , MAX(subq_21.booking_value) AS booking_value
    , MAX(subq_26.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_17.ds AS booking__ds__day
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.mf_time_spine subq_17
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      DATE_SUB(CAST(subq_17.ds AS DATETIME), INTERVAL 1 week) = DATETIME_TRUNC(bookings_source_src_28000.ds, day)
    GROUP BY
      booking__ds__day
  ) subq_21
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(ds, day) AS booking__ds__day
      , COUNT(DISTINCT guest_id) AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      booking__ds__day
  ) subq_26
  ON
    subq_21.booking__ds__day = subq_26.booking__ds__day
  GROUP BY
    booking__ds__day
) subq_27
