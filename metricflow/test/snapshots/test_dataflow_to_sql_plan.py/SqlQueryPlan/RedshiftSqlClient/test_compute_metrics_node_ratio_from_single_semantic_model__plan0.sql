-- Compute Metrics via Expressions
SELECT
  subq_5.listing
  , subq_5.listing__country_latest
  , CAST(subq_5.bookings AS DOUBLE PRECISION) / CAST(NULLIF(subq_5.bookers, 0) AS DOUBLE PRECISION) AS bookings_per_booker
FROM (
  -- Aggregate Measures
  SELECT
    subq_4.listing
    , subq_4.listing__country_latest
    , SUM(subq_4.bookings) AS bookings
    , COUNT(DISTINCT subq_4.bookers) AS bookers
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_1.listing AS listing
      , subq_3.country_latest AS listing__country_latest
      , subq_1.bookings AS bookings
      , subq_1.bookers AS bookers
    FROM (
      -- Pass Only Elements:
      --   ['bookings', 'bookers', 'listing']
      SELECT
        subq_0.listing
        , subq_0.bookings
        , subq_0.bookers
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS bookings
          , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
          , bookings_source_src_10001.booking_value
          , bookings_source_src_10001.booking_value AS max_booking_value
          , bookings_source_src_10001.booking_value AS min_booking_value
          , bookings_source_src_10001.guest_id AS bookers
          , bookings_source_src_10001.booking_value AS average_booking_value
          , bookings_source_src_10001.booking_value AS booking_payments
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
          , bookings_source_src_10001.booking_value AS median_booking_value
          , bookings_source_src_10001.booking_value AS booking_value_p99
          , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
          , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
          , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
          , bookings_source_src_10001.is_instant
          , bookings_source_src_10001.ds
          , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
          , bookings_source_src_10001.ds_partitioned
          , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
          , bookings_source_src_10001.booking_paid_at
          , DATE_TRUNC('week', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__week
          , DATE_TRUNC('month', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__year
          , bookings_source_src_10001.is_instant AS create_a_cycle_in_the_join_graph__is_instant
          , bookings_source_src_10001.ds AS create_a_cycle_in_the_join_graph__ds
          , DATE_TRUNC('week', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__year
          , bookings_source_src_10001.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
          , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
          , bookings_source_src_10001.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
          , DATE_TRUNC('week', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
          , DATE_TRUNC('month', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
          , bookings_source_src_10001.listing_id AS listing
          , bookings_source_src_10001.guest_id AS guest
          , bookings_source_src_10001.host_id AS host
          , bookings_source_src_10001.guest_id AS create_a_cycle_in_the_join_graph
          , bookings_source_src_10001.listing_id AS create_a_cycle_in_the_join_graph__listing
          , bookings_source_src_10001.guest_id AS create_a_cycle_in_the_join_graph__guest
          , bookings_source_src_10001.host_id AS create_a_cycle_in_the_join_graph__host
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_0
    ) subq_1
    LEFT OUTER JOIN (
      -- Pass Only Elements:
      --   ['country_latest', 'listing']
      SELECT
        subq_2.listing
        , subq_2.country_latest
      FROM (
        -- Read Elements From Semantic Model 'listings_latest'
        SELECT
          1 AS listings
          , listings_latest_src_10004.capacity AS largest_listing
          , listings_latest_src_10004.capacity AS smallest_listing
          , listings_latest_src_10004.created_at AS ds
          , DATE_TRUNC('week', listings_latest_src_10004.created_at) AS ds__week
          , DATE_TRUNC('month', listings_latest_src_10004.created_at) AS ds__month
          , DATE_TRUNC('quarter', listings_latest_src_10004.created_at) AS ds__quarter
          , DATE_TRUNC('year', listings_latest_src_10004.created_at) AS ds__year
          , listings_latest_src_10004.created_at
          , DATE_TRUNC('week', listings_latest_src_10004.created_at) AS created_at__week
          , DATE_TRUNC('month', listings_latest_src_10004.created_at) AS created_at__month
          , DATE_TRUNC('quarter', listings_latest_src_10004.created_at) AS created_at__quarter
          , DATE_TRUNC('year', listings_latest_src_10004.created_at) AS created_at__year
          , listings_latest_src_10004.country AS country_latest
          , listings_latest_src_10004.is_lux AS is_lux_latest
          , listings_latest_src_10004.capacity AS capacity_latest
          , listings_latest_src_10004.created_at AS listing__ds
          , DATE_TRUNC('week', listings_latest_src_10004.created_at) AS listing__ds__week
          , DATE_TRUNC('month', listings_latest_src_10004.created_at) AS listing__ds__month
          , DATE_TRUNC('quarter', listings_latest_src_10004.created_at) AS listing__ds__quarter
          , DATE_TRUNC('year', listings_latest_src_10004.created_at) AS listing__ds__year
          , listings_latest_src_10004.created_at AS listing__created_at
          , DATE_TRUNC('week', listings_latest_src_10004.created_at) AS listing__created_at__week
          , DATE_TRUNC('month', listings_latest_src_10004.created_at) AS listing__created_at__month
          , DATE_TRUNC('quarter', listings_latest_src_10004.created_at) AS listing__created_at__quarter
          , DATE_TRUNC('year', listings_latest_src_10004.created_at) AS listing__created_at__year
          , listings_latest_src_10004.country AS listing__country_latest
          , listings_latest_src_10004.is_lux AS listing__is_lux_latest
          , listings_latest_src_10004.capacity AS listing__capacity_latest
          , listings_latest_src_10004.listing_id AS listing
          , listings_latest_src_10004.user_id AS user
          , listings_latest_src_10004.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_10004
      ) subq_2
    ) subq_3
    ON
      subq_1.listing = subq_3.listing
  ) subq_4
  GROUP BY
    subq_4.listing
    , subq_4.listing__country_latest
) subq_5
