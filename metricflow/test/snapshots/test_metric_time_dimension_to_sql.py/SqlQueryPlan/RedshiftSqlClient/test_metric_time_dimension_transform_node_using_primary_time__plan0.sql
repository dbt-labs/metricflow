-- Metric Time Dimension 'ds'
SELECT
  subq_0.ds
  , subq_0.ds__week
  , subq_0.ds__month
  , subq_0.ds__quarter
  , subq_0.ds__year
  , subq_0.ds_partitioned
  , subq_0.ds_partitioned__week
  , subq_0.ds_partitioned__month
  , subq_0.ds_partitioned__quarter
  , subq_0.ds_partitioned__year
  , subq_0.booking_paid_at
  , subq_0.booking_paid_at__week
  , subq_0.booking_paid_at__month
  , subq_0.booking_paid_at__quarter
  , subq_0.booking_paid_at__year
  , subq_0.create_a_cycle_in_the_join_graph__ds
  , subq_0.create_a_cycle_in_the_join_graph__ds__week
  , subq_0.create_a_cycle_in_the_join_graph__ds__month
  , subq_0.create_a_cycle_in_the_join_graph__ds__quarter
  , subq_0.create_a_cycle_in_the_join_graph__ds__year
  , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned
  , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__week
  , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__month
  , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
  , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__year
  , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at
  , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__week
  , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__month
  , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
  , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__year
  , subq_0.ds AS metric_time
  , subq_0.ds__week AS metric_time__week
  , subq_0.ds__month AS metric_time__month
  , subq_0.ds__quarter AS metric_time__quarter
  , subq_0.ds__year AS metric_time__year
  , subq_0.listing
  , subq_0.guest
  , subq_0.host
  , subq_0.create_a_cycle_in_the_join_graph
  , subq_0.create_a_cycle_in_the_join_graph__listing
  , subq_0.create_a_cycle_in_the_join_graph__guest
  , subq_0.create_a_cycle_in_the_join_graph__host
  , subq_0.is_instant
  , subq_0.create_a_cycle_in_the_join_graph__is_instant
  , subq_0.bookings
  , subq_0.instant_bookings
  , subq_0.booking_value
  , subq_0.max_booking_value
  , subq_0.min_booking_value
  , subq_0.bookers
  , subq_0.average_booking_value
FROM (
  -- Read Elements From Data Source 'bookings_source'
  SELECT
    1 AS bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    , bookings_source_src_10000.booking_value
    , bookings_source_src_10000.booking_value AS max_booking_value
    , bookings_source_src_10000.booking_value AS min_booking_value
    , bookings_source_src_10000.guest_id AS bookers
    , bookings_source_src_10000.booking_value AS average_booking_value
    , bookings_source_src_10000.booking_value AS booking_payments
    , bookings_source_src_10000.is_instant
    , bookings_source_src_10000.ds
    , DATE_TRUNC('week', bookings_source_src_10000.ds) AS ds__week
    , DATE_TRUNC('month', bookings_source_src_10000.ds) AS ds__month
    , DATE_TRUNC('quarter', bookings_source_src_10000.ds) AS ds__quarter
    , DATE_TRUNC('year', bookings_source_src_10000.ds) AS ds__year
    , bookings_source_src_10000.ds_partitioned
    , DATE_TRUNC('week', bookings_source_src_10000.ds_partitioned) AS ds_partitioned__week
    , DATE_TRUNC('month', bookings_source_src_10000.ds_partitioned) AS ds_partitioned__month
    , DATE_TRUNC('quarter', bookings_source_src_10000.ds_partitioned) AS ds_partitioned__quarter
    , DATE_TRUNC('year', bookings_source_src_10000.ds_partitioned) AS ds_partitioned__year
    , bookings_source_src_10000.booking_paid_at
    , DATE_TRUNC('week', bookings_source_src_10000.booking_paid_at) AS booking_paid_at__week
    , DATE_TRUNC('month', bookings_source_src_10000.booking_paid_at) AS booking_paid_at__month
    , DATE_TRUNC('quarter', bookings_source_src_10000.booking_paid_at) AS booking_paid_at__quarter
    , DATE_TRUNC('year', bookings_source_src_10000.booking_paid_at) AS booking_paid_at__year
    , bookings_source_src_10000.is_instant AS create_a_cycle_in_the_join_graph__is_instant
    , bookings_source_src_10000.ds AS create_a_cycle_in_the_join_graph__ds
    , DATE_TRUNC('week', bookings_source_src_10000.ds) AS create_a_cycle_in_the_join_graph__ds__week
    , DATE_TRUNC('month', bookings_source_src_10000.ds) AS create_a_cycle_in_the_join_graph__ds__month
    , DATE_TRUNC('quarter', bookings_source_src_10000.ds) AS create_a_cycle_in_the_join_graph__ds__quarter
    , DATE_TRUNC('year', bookings_source_src_10000.ds) AS create_a_cycle_in_the_join_graph__ds__year
    , bookings_source_src_10000.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
    , DATE_TRUNC('week', bookings_source_src_10000.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
    , DATE_TRUNC('month', bookings_source_src_10000.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
    , DATE_TRUNC('quarter', bookings_source_src_10000.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
    , DATE_TRUNC('year', bookings_source_src_10000.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
    , bookings_source_src_10000.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
    , DATE_TRUNC('week', bookings_source_src_10000.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
    , DATE_TRUNC('month', bookings_source_src_10000.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
    , DATE_TRUNC('quarter', bookings_source_src_10000.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
    , DATE_TRUNC('year', bookings_source_src_10000.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
    , bookings_source_src_10000.listing_id AS listing
    , bookings_source_src_10000.guest_id AS guest
    , bookings_source_src_10000.host_id AS host
    , bookings_source_src_10000.guest_id AS create_a_cycle_in_the_join_graph
    , bookings_source_src_10000.listing_id AS create_a_cycle_in_the_join_graph__listing
    , bookings_source_src_10000.guest_id AS create_a_cycle_in_the_join_graph__guest
    , bookings_source_src_10000.host_id AS create_a_cycle_in_the_join_graph__host
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
) subq_0
