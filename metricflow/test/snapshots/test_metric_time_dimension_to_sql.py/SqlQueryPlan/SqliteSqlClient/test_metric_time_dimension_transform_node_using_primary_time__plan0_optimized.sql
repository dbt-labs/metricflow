-- Read Elements From Data Source 'bookings_source'
-- Metric Time Dimension 'ds'
SELECT
  1 AS bookings
  , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
  , booking_value
  , booking_value AS max_booking_value
  , booking_value AS min_booking_value
  , guest_id AS bookers
  , booking_value AS average_booking_value
  , is_instant
  , is_instant AS create_a_cycle_in_the_join_graph__is_instant
  , ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
  , ds_partitioned
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
  , booking_paid_at
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__year
  , ds AS create_a_cycle_in_the_join_graph__ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__year
  , ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__year
  , booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__year
  , ds AS metric_time
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS metric_time__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS metric_time__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS metric_time__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS metric_time__year
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , guest_id AS create_a_cycle_in_the_join_graph
  , listing_id AS create_a_cycle_in_the_join_graph__listing
  , guest_id AS create_a_cycle_in_the_join_graph__guest
  , host_id AS create_a_cycle_in_the_join_graph__host
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_bookings
) bookings_source_src_10000
