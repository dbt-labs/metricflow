-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'ds'
SELECT
  ds
  , DATE_TRUNC('week', ds) AS ds__week
  , DATE_TRUNC('month', ds) AS ds__month
  , DATE_TRUNC('quarter', ds) AS ds__quarter
  , DATE_TRUNC('year', ds) AS ds__year
  , ds_partitioned
  , DATE_TRUNC('week', ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS ds_partitioned__year
  , booking_paid_at
  , DATE_TRUNC('week', booking_paid_at) AS booking_paid_at__week
  , DATE_TRUNC('month', booking_paid_at) AS booking_paid_at__month
  , DATE_TRUNC('quarter', booking_paid_at) AS booking_paid_at__quarter
  , DATE_TRUNC('year', booking_paid_at) AS booking_paid_at__year
  , ds AS create_a_cycle_in_the_join_graph__ds
  , DATE_TRUNC('week', ds) AS create_a_cycle_in_the_join_graph__ds__week
  , DATE_TRUNC('month', ds) AS create_a_cycle_in_the_join_graph__ds__month
  , DATE_TRUNC('quarter', ds) AS create_a_cycle_in_the_join_graph__ds__quarter
  , DATE_TRUNC('year', ds) AS create_a_cycle_in_the_join_graph__ds__year
  , ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
  , DATE_TRUNC('week', ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
  , booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
  , DATE_TRUNC('week', booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
  , DATE_TRUNC('month', booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
  , DATE_TRUNC('quarter', booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
  , DATE_TRUNC('year', booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
  , ds AS metric_time
  , DATE_TRUNC('week', ds) AS metric_time__week
  , DATE_TRUNC('month', ds) AS metric_time__month
  , DATE_TRUNC('quarter', ds) AS metric_time__quarter
  , DATE_TRUNC('year', ds) AS metric_time__year
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
