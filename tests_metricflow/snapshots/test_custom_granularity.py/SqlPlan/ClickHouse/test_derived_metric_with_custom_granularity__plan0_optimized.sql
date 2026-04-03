test_name: test_derived_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  booking__ds__alien_day
  , booking_value * 0.05 / bookers AS booking_fees_per_booker
FROM (
  SELECT
    subq_16.alien_day AS booking__ds__alien_day
    , SUM(bookings_source_src_28000.booking_value) AS booking_value
    , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_16
  ON
    toStartOfDay(bookings_source_src_28000.ds) = subq_16.ds
  GROUP BY
    subq_16.alien_day
) subq_21
