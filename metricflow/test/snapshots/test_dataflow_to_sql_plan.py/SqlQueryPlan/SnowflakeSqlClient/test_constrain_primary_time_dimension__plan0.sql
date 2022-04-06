-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
SELECT
  subq_1.bookings
  , subq_1.ds
FROM (
  -- Pass Only Elements:
  --   ['bookings', 'ds']
  SELECT
    subq_0.bookings
    , subq_0.ds
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
) subq_1
WHERE (
  subq_1.ds >= CAST('2020-01-01' AS TIMESTAMP)
) AND (
  subq_1.ds <= CAST('2020-01-02' AS TIMESTAMP)
)
