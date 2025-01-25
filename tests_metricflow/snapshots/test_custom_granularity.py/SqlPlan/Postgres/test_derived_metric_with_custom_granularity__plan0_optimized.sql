test_name: test_derived_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__martian_day
  , booking_value * 0.05 / bookers AS booking_fees_per_booker
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['booking_value', 'bookers', 'booking__ds__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    nr_subq_12.martian_day AS booking__ds__martian_day
    , SUM(bookings_source_src_28000.booking_value) AS booking_value
    , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_12
  ON
    DATE_TRUNC('day', bookings_source_src_28000.ds) = nr_subq_12.ds
  GROUP BY
    nr_subq_12.martian_day
) nr_subq_16
