-- Compute Metrics via Expressions
SELECT
  subq_5.listing
  , subq_5.listing__country_latest
  , subq_5.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_4.listing
    , subq_4.listing__country_latest
    , SUM(subq_4.bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_1.listing AS listing
      , subq_3.country_latest AS listing__country_latest
      , subq_1.bookings AS bookings
    FROM (
      -- Pass Only Elements:
      --   ['bookings', 'listing']
      SELECT
        subq_0.listing
        , subq_0.bookings
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
          , DATE_TRUNC(bookings_source_src_10000.ds, isoweek) AS ds__week
          , DATE_TRUNC(bookings_source_src_10000.ds, month) AS ds__month
          , DATE_TRUNC(bookings_source_src_10000.ds, quarter) AS ds__quarter
          , DATE_TRUNC(bookings_source_src_10000.ds, isoyear) AS ds__year
          , bookings_source_src_10000.ds_partitioned
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoweek) AS ds_partitioned__week
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, month) AS ds_partitioned__month
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, quarter) AS ds_partitioned__quarter
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoyear) AS ds_partitioned__year
          , bookings_source_src_10000.booking_paid_at
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoweek) AS booking_paid_at__week
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, month) AS booking_paid_at__month
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, quarter) AS booking_paid_at__quarter
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoyear) AS booking_paid_at__year
          , bookings_source_src_10000.is_instant AS create_a_cycle_in_the_join_graph__is_instant
          , bookings_source_src_10000.ds AS create_a_cycle_in_the_join_graph__ds
          , DATE_TRUNC(bookings_source_src_10000.ds, isoweek) AS create_a_cycle_in_the_join_graph__ds__week
          , DATE_TRUNC(bookings_source_src_10000.ds, month) AS create_a_cycle_in_the_join_graph__ds__month
          , DATE_TRUNC(bookings_source_src_10000.ds, quarter) AS create_a_cycle_in_the_join_graph__ds__quarter
          , DATE_TRUNC(bookings_source_src_10000.ds, isoyear) AS create_a_cycle_in_the_join_graph__ds__year
          , bookings_source_src_10000.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoweek) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, month) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, quarter) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
          , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoyear) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
          , bookings_source_src_10000.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoweek) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, month) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, quarter) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
          , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoyear) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
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
    LEFT OUTER JOIN (
      -- Pass Only Elements:
      --   ['listing', 'country_latest']
      SELECT
        subq_2.listing
        , subq_2.country_latest
      FROM (
        -- Read Elements From Data Source 'listings_latest'
        SELECT
          1 AS listings
          , listings_latest_src_10003.capacity AS largest_listing
          , listings_latest_src_10003.capacity AS smallest_listing
          , listings_latest_src_10003.created_at AS ds
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS ds__week
          , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS ds__month
          , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS ds__quarter
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS ds__year
          , listings_latest_src_10003.created_at
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS created_at__week
          , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS created_at__month
          , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS created_at__quarter
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS created_at__year
          , listings_latest_src_10003.country AS country_latest
          , listings_latest_src_10003.is_lux AS is_lux_latest
          , listings_latest_src_10003.capacity AS capacity_latest
          , listings_latest_src_10003.created_at AS listing__ds
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS listing__ds__week
          , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS listing__ds__month
          , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS listing__ds__quarter
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS listing__ds__year
          , listings_latest_src_10003.created_at AS listing__created_at
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS listing__created_at__week
          , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS listing__created_at__month
          , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS listing__created_at__quarter
          , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS listing__created_at__year
          , listings_latest_src_10003.country AS listing__country_latest
          , listings_latest_src_10003.is_lux AS listing__is_lux_latest
          , listings_latest_src_10003.capacity AS listing__capacity_latest
          , listings_latest_src_10003.listing_id AS listing
          , listings_latest_src_10003.user_id AS user
          , listings_latest_src_10003.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_10003
      ) subq_2
    ) subq_3
    ON
      subq_1.listing = subq_3.listing
  ) subq_4
  GROUP BY
    subq_4.listing
    , subq_4.listing__country_latest
) subq_5
