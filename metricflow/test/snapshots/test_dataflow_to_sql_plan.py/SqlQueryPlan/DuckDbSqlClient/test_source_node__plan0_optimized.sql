-- Read Elements From Data Source 'bookings_source'
SELECT
  1 AS bookings
  , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
  , booking_value
  , booking_value AS max_booking_value
  , booking_value AS min_booking_value
  , guest_id AS bookers
  , booking_value AS average_booking_value
  , is_instant
  , ds
  , DATE_TRUNC('week', ds) AS ds__week
  , DATE_TRUNC('month', ds) AS ds__month
  , DATE_TRUNC('quarter', ds) AS ds__quarter
  , DATE_TRUNC('year', ds) AS ds__year
  , ds_partitioned
  , DATE_TRUNC('week', ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS ds_partitioned__year
  , is_instant AS create_a_cycle_in_the_join_graph__is_instant
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
