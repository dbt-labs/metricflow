-- Combine Metrics
SELECT
  MAX(subq_5.bookings) AS bookings
  , MAX(subq_11.listings) AS listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_4.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_3.bookings) AS bookings
    FROM (
      -- Pass Only Elements:
      --   ['bookings']
      SELECT
        subq_2.bookings
      FROM (
        -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
        SELECT
          subq_1.ds
          , subq_1.ds__week
          , subq_1.ds__month
          , subq_1.ds__quarter
          , subq_1.ds__year
          , subq_1.ds_partitioned
          , subq_1.ds_partitioned__week
          , subq_1.ds_partitioned__month
          , subq_1.ds_partitioned__quarter
          , subq_1.ds_partitioned__year
          , subq_1.booking_paid_at
          , subq_1.booking_paid_at__week
          , subq_1.booking_paid_at__month
          , subq_1.booking_paid_at__quarter
          , subq_1.booking_paid_at__year
          , subq_1.create_a_cycle_in_the_join_graph__ds
          , subq_1.create_a_cycle_in_the_join_graph__ds__week
          , subq_1.create_a_cycle_in_the_join_graph__ds__month
          , subq_1.create_a_cycle_in_the_join_graph__ds__quarter
          , subq_1.create_a_cycle_in_the_join_graph__ds__year
          , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned
          , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__week
          , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__month
          , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
          , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__year
          , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at
          , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__week
          , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__month
          , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
          , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__year
          , subq_1.metric_time
          , subq_1.metric_time__week
          , subq_1.metric_time__month
          , subq_1.metric_time__quarter
          , subq_1.metric_time__year
          , subq_1.listing
          , subq_1.guest
          , subq_1.host
          , subq_1.create_a_cycle_in_the_join_graph
          , subq_1.create_a_cycle_in_the_join_graph__listing
          , subq_1.create_a_cycle_in_the_join_graph__guest
          , subq_1.create_a_cycle_in_the_join_graph__host
          , subq_1.is_instant
          , subq_1.create_a_cycle_in_the_join_graph__is_instant
          , subq_1.bookings
          , subq_1.instant_bookings
          , subq_1.booking_value
          , subq_1.max_booking_value
          , subq_1.min_booking_value
          , subq_1.bookers
          , subq_1.average_booking_value
          , subq_1.referred_bookings
          , subq_1.median_booking_value
          , subq_1.booking_value_p99
          , subq_1.discrete_booking_value_p99
          , subq_1.approximate_continuous_booking_value_p99
          , subq_1.approximate_discrete_booking_value_p99
        FROM (
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
            , subq_0.referred_bookings
            , subq_0.median_booking_value
            , subq_0.booking_value_p99
            , subq_0.discrete_booking_value_p99
            , subq_0.approximate_continuous_booking_value_p99
            , subq_0.approximate_discrete_booking_value_p99
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
        WHERE subq_1.metric_time BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-01' AS TIMESTAMP)
      ) subq_2
    ) subq_3
  ) subq_4
) subq_5
CROSS JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_10.listings
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_9.listings) AS listings
    FROM (
      -- Pass Only Elements:
      --   ['listings']
      SELECT
        subq_8.listings
      FROM (
        -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
        SELECT
          subq_7.ds
          , subq_7.ds__week
          , subq_7.ds__month
          , subq_7.ds__quarter
          , subq_7.ds__year
          , subq_7.created_at
          , subq_7.created_at__week
          , subq_7.created_at__month
          , subq_7.created_at__quarter
          , subq_7.created_at__year
          , subq_7.listing__ds
          , subq_7.listing__ds__week
          , subq_7.listing__ds__month
          , subq_7.listing__ds__quarter
          , subq_7.listing__ds__year
          , subq_7.listing__created_at
          , subq_7.listing__created_at__week
          , subq_7.listing__created_at__month
          , subq_7.listing__created_at__quarter
          , subq_7.listing__created_at__year
          , subq_7.metric_time
          , subq_7.metric_time__week
          , subq_7.metric_time__month
          , subq_7.metric_time__quarter
          , subq_7.metric_time__year
          , subq_7.listing
          , subq_7.user
          , subq_7.listing__user
          , subq_7.country_latest
          , subq_7.is_lux_latest
          , subq_7.capacity_latest
          , subq_7.listing__country_latest
          , subq_7.listing__is_lux_latest
          , subq_7.listing__capacity_latest
          , subq_7.listings
          , subq_7.largest_listing
          , subq_7.smallest_listing
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_6.ds
            , subq_6.ds__week
            , subq_6.ds__month
            , subq_6.ds__quarter
            , subq_6.ds__year
            , subq_6.created_at
            , subq_6.created_at__week
            , subq_6.created_at__month
            , subq_6.created_at__quarter
            , subq_6.created_at__year
            , subq_6.listing__ds
            , subq_6.listing__ds__week
            , subq_6.listing__ds__month
            , subq_6.listing__ds__quarter
            , subq_6.listing__ds__year
            , subq_6.listing__created_at
            , subq_6.listing__created_at__week
            , subq_6.listing__created_at__month
            , subq_6.listing__created_at__quarter
            , subq_6.listing__created_at__year
            , subq_6.ds AS metric_time
            , subq_6.ds__week AS metric_time__week
            , subq_6.ds__month AS metric_time__month
            , subq_6.ds__quarter AS metric_time__quarter
            , subq_6.ds__year AS metric_time__year
            , subq_6.listing
            , subq_6.user
            , subq_6.listing__user
            , subq_6.country_latest
            , subq_6.is_lux_latest
            , subq_6.capacity_latest
            , subq_6.listing__country_latest
            , subq_6.listing__is_lux_latest
            , subq_6.listing__capacity_latest
            , subq_6.listings
            , subq_6.largest_listing
            , subq_6.smallest_listing
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
          ) subq_6
        ) subq_7
        WHERE subq_7.metric_time BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-01' AS TIMESTAMP)
      ) subq_8
    ) subq_9
  ) subq_10
) subq_11
