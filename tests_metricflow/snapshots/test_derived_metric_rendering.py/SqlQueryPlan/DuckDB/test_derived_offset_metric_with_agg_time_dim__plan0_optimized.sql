test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.booking__ds__day, subq_28.booking__ds__day) AS booking__ds__day
    , MAX(subq_23.booking_value) AS booking_value
    , MAX(subq_28.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS booking__ds__day
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      time_spine_src_28006.ds - INTERVAL 1 week = DATE_TRUNC('day', bookings_source_src_28000.ds)
    GROUP BY
      time_spine_src_28006.ds
  ) subq_23
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
  ) subq_28
  ON
    subq_23.booking__ds__day = subq_28.booking__ds__day
  GROUP BY
    COALESCE(subq_23.booking__ds__day, subq_28.booking__ds__day)
) subq_29
