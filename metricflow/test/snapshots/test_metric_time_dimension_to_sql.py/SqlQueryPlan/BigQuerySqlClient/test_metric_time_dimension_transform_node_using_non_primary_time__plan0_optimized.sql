-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'booking_paid_at'
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
  , booking_paid_at AS metric_time
  , DATE_TRUNC(booking_paid_at, isoweek) AS metric_time__week
  , DATE_TRUNC(booking_paid_at, month) AS metric_time__month
  , DATE_TRUNC(booking_paid_at, quarter) AS metric_time__quarter
  , DATE_TRUNC(booking_paid_at, isoyear) AS metric_time__year
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , guest_id AS create_a_cycle_in_the_join_graph
  , listing_id AS create_a_cycle_in_the_join_graph__listing
  , guest_id AS create_a_cycle_in_the_join_graph__guest
  , host_id AS create_a_cycle_in_the_join_graph__host
  , is_instant
  , is_instant AS create_a_cycle_in_the_join_graph__is_instant
  , booking_value AS booking_payments
FROM ***************************.fct_bookings bookings_source_src_10001
