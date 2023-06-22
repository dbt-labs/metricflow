-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'ds'
SELECT
  ds
  , DATE_TRUNC(ds, isoweek) AS ds__week
  , DATE_TRUNC(ds, month) AS ds__month
  , DATE_TRUNC(ds, quarter) AS ds__quarter
  , DATE_TRUNC(ds, isoyear) AS ds__year
  , ds_partitioned
  , DATE_TRUNC(ds_partitioned, isoweek) AS ds_partitioned__week
  , DATE_TRUNC(ds_partitioned, month) AS ds_partitioned__month
  , DATE_TRUNC(ds_partitioned, quarter) AS ds_partitioned__quarter
  , DATE_TRUNC(ds_partitioned, isoyear) AS ds_partitioned__year
  , booking_paid_at
  , DATE_TRUNC(booking_paid_at, isoweek) AS booking_paid_at__week
  , DATE_TRUNC(booking_paid_at, month) AS booking_paid_at__month
  , DATE_TRUNC(booking_paid_at, quarter) AS booking_paid_at__quarter
  , DATE_TRUNC(booking_paid_at, isoyear) AS booking_paid_at__year
  , ds AS create_a_cycle_in_the_join_graph__ds
  , DATE_TRUNC(ds, isoweek) AS create_a_cycle_in_the_join_graph__ds__week
  , DATE_TRUNC(ds, month) AS create_a_cycle_in_the_join_graph__ds__month
  , DATE_TRUNC(ds, quarter) AS create_a_cycle_in_the_join_graph__ds__quarter
  , DATE_TRUNC(ds, isoyear) AS create_a_cycle_in_the_join_graph__ds__year
  , ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
  , DATE_TRUNC(ds_partitioned, isoweek) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
  , DATE_TRUNC(ds_partitioned, month) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
  , DATE_TRUNC(ds_partitioned, quarter) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
  , DATE_TRUNC(ds_partitioned, isoyear) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
  , booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
  , DATE_TRUNC(booking_paid_at, isoweek) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
  , DATE_TRUNC(booking_paid_at, month) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
  , DATE_TRUNC(booking_paid_at, quarter) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
  , DATE_TRUNC(booking_paid_at, isoyear) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
  , ds AS metric_time
  , DATE_TRUNC(ds, isoweek) AS metric_time__week
  , DATE_TRUNC(ds, month) AS metric_time__month
  , DATE_TRUNC(ds, quarter) AS metric_time__quarter
  , DATE_TRUNC(ds, isoyear) AS metric_time__year
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , guest_id AS create_a_cycle_in_the_join_graph
  , listing_id AS create_a_cycle_in_the_join_graph__listing
  , guest_id AS create_a_cycle_in_the_join_graph__guest
  , host_id AS create_a_cycle_in_the_join_graph__host
  , is_instant
  , is_instant AS create_a_cycle_in_the_join_graph__is_instant
  , 1 AS bookings
  , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
  , booking_value
  , booking_value AS max_booking_value
  , booking_value AS min_booking_value
  , guest_id AS bookers
  , booking_value AS average_booking_value
  , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
  , booking_value AS median_booking_value
  , booking_value AS booking_value_p99
  , booking_value AS discrete_booking_value_p99
  , booking_value AS approximate_continuous_booking_value_p99
  , booking_value AS approximate_discrete_booking_value_p99
FROM ***************************.fct_bookings bookings_source_src_10001
