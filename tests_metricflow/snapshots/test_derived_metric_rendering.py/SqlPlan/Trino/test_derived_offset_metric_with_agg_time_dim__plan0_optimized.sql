test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_20.booking__ds__day, nr_subq_24.booking__ds__day) AS booking__ds__day
    , MAX(nr_subq_20.booking_value) AS booking_value
    , MAX(nr_subq_24.bookers) AS bookers
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
      DATE_ADD('week', -1, time_spine_src_28006.ds) = DATE_TRUNC('day', bookings_source_src_28000.ds)
    GROUP BY
      time_spine_src_28006.ds
  ) nr_subq_20
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
  ) nr_subq_24
  ON
    nr_subq_20.booking__ds__day = nr_subq_24.booking__ds__day
  GROUP BY
    COALESCE(nr_subq_20.booking__ds__day, nr_subq_24.booking__ds__day)
) nr_subq_25
