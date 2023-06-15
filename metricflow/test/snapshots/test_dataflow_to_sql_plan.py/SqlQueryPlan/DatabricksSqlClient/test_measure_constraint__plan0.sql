-- Compute Metrics via Expressions
SELECT
  subq_29.metric_time
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_11.metric_time, subq_23.metric_time, subq_28.metric_time) AS metric_time
    , subq_11.average_booking_value AS average_booking_value
    , subq_23.bookings AS bookings
    , subq_28.booking_value AS booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_10.metric_time
      , subq_10.average_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_9.metric_time
        , AVG(subq_9.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements:
        --   ['average_booking_value', 'metric_time']
        SELECT
          subq_8.metric_time
          , subq_8.average_booking_value
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_7.metric_time
            , subq_7.listing__is_lux_latest
            , subq_7.average_booking_value
          FROM (
            -- Pass Only Elements:
            --   ['average_booking_value', 'listing__is_lux_latest', 'metric_time']
            SELECT
              subq_6.metric_time
              , subq_6.listing__is_lux_latest
              , subq_6.average_booking_value
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_2.metric_time AS metric_time
                , subq_2.listing AS listing
                , subq_5.is_lux_latest AS listing__is_lux_latest
                , subq_2.average_booking_value AS average_booking_value
              FROM (
                -- Pass Only Elements:
                --   ['average_booking_value', 'metric_time', 'listing']
                SELECT
                  subq_1.metric_time
                  , subq_1.listing
                  , subq_1.average_booking_value
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
              ) subq_2
              LEFT OUTER JOIN (
                -- Pass Only Elements:
                --   ['is_lux_latest', 'listing']
                SELECT
                  subq_4.listing
                  , subq_4.is_lux_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_3.ds
                    , subq_3.ds__week
                    , subq_3.ds__month
                    , subq_3.ds__quarter
                    , subq_3.ds__year
                    , subq_3.created_at
                    , subq_3.created_at__week
                    , subq_3.created_at__month
                    , subq_3.created_at__quarter
                    , subq_3.created_at__year
                    , subq_3.listing__ds
                    , subq_3.listing__ds__week
                    , subq_3.listing__ds__month
                    , subq_3.listing__ds__quarter
                    , subq_3.listing__ds__year
                    , subq_3.listing__created_at
                    , subq_3.listing__created_at__week
                    , subq_3.listing__created_at__month
                    , subq_3.listing__created_at__quarter
                    , subq_3.listing__created_at__year
                    , subq_3.ds AS metric_time
                    , subq_3.ds__week AS metric_time__week
                    , subq_3.ds__month AS metric_time__month
                    , subq_3.ds__quarter AS metric_time__quarter
                    , subq_3.ds__year AS metric_time__year
                    , subq_3.listing
                    , subq_3.user
                    , subq_3.listing__user
                    , subq_3.country_latest
                    , subq_3.is_lux_latest
                    , subq_3.capacity_latest
                    , subq_3.listing__country_latest
                    , subq_3.listing__is_lux_latest
                    , subq_3.listing__capacity_latest
                    , subq_3.listings
                    , subq_3.largest_listing
                    , subq_3.smallest_listing
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
                  ) subq_3
                ) subq_4
              ) subq_5
              ON
                subq_2.listing = subq_5.listing
            ) subq_6
          ) subq_7
          WHERE listing__is_lux_latest
        ) subq_8
      ) subq_9
      GROUP BY
        subq_9.metric_time
    ) subq_10
  ) subq_11
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_22.metric_time
      , subq_22.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_21.metric_time
        , SUM(subq_21.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          subq_20.metric_time
          , subq_20.bookings
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_19.metric_time
            , subq_19.listing__is_lux_latest
            , subq_19.bookings
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'listing__is_lux_latest', 'metric_time']
            SELECT
              subq_18.metric_time
              , subq_18.listing__is_lux_latest
              , subq_18.bookings
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_14.metric_time AS metric_time
                , subq_14.listing AS listing
                , subq_17.is_lux_latest AS listing__is_lux_latest
                , subq_14.bookings AS bookings
              FROM (
                -- Pass Only Elements:
                --   ['bookings', 'metric_time', 'listing']
                SELECT
                  subq_13.metric_time
                  , subq_13.listing
                  , subq_13.bookings
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_12.ds
                    , subq_12.ds__week
                    , subq_12.ds__month
                    , subq_12.ds__quarter
                    , subq_12.ds__year
                    , subq_12.ds_partitioned
                    , subq_12.ds_partitioned__week
                    , subq_12.ds_partitioned__month
                    , subq_12.ds_partitioned__quarter
                    , subq_12.ds_partitioned__year
                    , subq_12.booking_paid_at
                    , subq_12.booking_paid_at__week
                    , subq_12.booking_paid_at__month
                    , subq_12.booking_paid_at__quarter
                    , subq_12.booking_paid_at__year
                    , subq_12.create_a_cycle_in_the_join_graph__ds
                    , subq_12.create_a_cycle_in_the_join_graph__ds__week
                    , subq_12.create_a_cycle_in_the_join_graph__ds__month
                    , subq_12.create_a_cycle_in_the_join_graph__ds__quarter
                    , subq_12.create_a_cycle_in_the_join_graph__ds__year
                    , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned
                    , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__week
                    , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__month
                    , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                    , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__year
                    , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at
                    , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__week
                    , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__month
                    , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
                    , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__year
                    , subq_12.ds AS metric_time
                    , subq_12.ds__week AS metric_time__week
                    , subq_12.ds__month AS metric_time__month
                    , subq_12.ds__quarter AS metric_time__quarter
                    , subq_12.ds__year AS metric_time__year
                    , subq_12.listing
                    , subq_12.guest
                    , subq_12.host
                    , subq_12.create_a_cycle_in_the_join_graph
                    , subq_12.create_a_cycle_in_the_join_graph__listing
                    , subq_12.create_a_cycle_in_the_join_graph__guest
                    , subq_12.create_a_cycle_in_the_join_graph__host
                    , subq_12.is_instant
                    , subq_12.create_a_cycle_in_the_join_graph__is_instant
                    , subq_12.bookings
                    , subq_12.instant_bookings
                    , subq_12.booking_value
                    , subq_12.max_booking_value
                    , subq_12.min_booking_value
                    , subq_12.bookers
                    , subq_12.average_booking_value
                    , subq_12.referred_bookings
                    , subq_12.median_booking_value
                    , subq_12.booking_value_p99
                    , subq_12.discrete_booking_value_p99
                    , subq_12.approximate_continuous_booking_value_p99
                    , subq_12.approximate_discrete_booking_value_p99
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
                  ) subq_12
                ) subq_13
              ) subq_14
              LEFT OUTER JOIN (
                -- Pass Only Elements:
                --   ['is_lux_latest', 'listing']
                SELECT
                  subq_16.listing
                  , subq_16.is_lux_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_15.ds
                    , subq_15.ds__week
                    , subq_15.ds__month
                    , subq_15.ds__quarter
                    , subq_15.ds__year
                    , subq_15.created_at
                    , subq_15.created_at__week
                    , subq_15.created_at__month
                    , subq_15.created_at__quarter
                    , subq_15.created_at__year
                    , subq_15.listing__ds
                    , subq_15.listing__ds__week
                    , subq_15.listing__ds__month
                    , subq_15.listing__ds__quarter
                    , subq_15.listing__ds__year
                    , subq_15.listing__created_at
                    , subq_15.listing__created_at__week
                    , subq_15.listing__created_at__month
                    , subq_15.listing__created_at__quarter
                    , subq_15.listing__created_at__year
                    , subq_15.ds AS metric_time
                    , subq_15.ds__week AS metric_time__week
                    , subq_15.ds__month AS metric_time__month
                    , subq_15.ds__quarter AS metric_time__quarter
                    , subq_15.ds__year AS metric_time__year
                    , subq_15.listing
                    , subq_15.user
                    , subq_15.listing__user
                    , subq_15.country_latest
                    , subq_15.is_lux_latest
                    , subq_15.capacity_latest
                    , subq_15.listing__country_latest
                    , subq_15.listing__is_lux_latest
                    , subq_15.listing__capacity_latest
                    , subq_15.listings
                    , subq_15.largest_listing
                    , subq_15.smallest_listing
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
                  ) subq_15
                ) subq_16
              ) subq_17
              ON
                subq_14.listing = subq_17.listing
            ) subq_18
          ) subq_19
          WHERE listing__is_lux_latest
        ) subq_20
      ) subq_21
      GROUP BY
        subq_21.metric_time
    ) subq_22
  ) subq_23
  ON
    (
      subq_11.metric_time = subq_23.metric_time
    ) OR (
      (subq_11.metric_time IS NULL) AND (subq_23.metric_time IS NULL)
    )
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_27.metric_time
      , subq_27.booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_26.metric_time
        , SUM(subq_26.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time']
        SELECT
          subq_25.metric_time
          , subq_25.booking_value
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_24.ds
            , subq_24.ds__week
            , subq_24.ds__month
            , subq_24.ds__quarter
            , subq_24.ds__year
            , subq_24.ds_partitioned
            , subq_24.ds_partitioned__week
            , subq_24.ds_partitioned__month
            , subq_24.ds_partitioned__quarter
            , subq_24.ds_partitioned__year
            , subq_24.booking_paid_at
            , subq_24.booking_paid_at__week
            , subq_24.booking_paid_at__month
            , subq_24.booking_paid_at__quarter
            , subq_24.booking_paid_at__year
            , subq_24.create_a_cycle_in_the_join_graph__ds
            , subq_24.create_a_cycle_in_the_join_graph__ds__week
            , subq_24.create_a_cycle_in_the_join_graph__ds__month
            , subq_24.create_a_cycle_in_the_join_graph__ds__quarter
            , subq_24.create_a_cycle_in_the_join_graph__ds__year
            , subq_24.create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_24.create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_24.create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_24.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_24.create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_24.create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_24.create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_24.create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_24.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_24.create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_24.ds AS metric_time
            , subq_24.ds__week AS metric_time__week
            , subq_24.ds__month AS metric_time__month
            , subq_24.ds__quarter AS metric_time__quarter
            , subq_24.ds__year AS metric_time__year
            , subq_24.listing
            , subq_24.guest
            , subq_24.host
            , subq_24.create_a_cycle_in_the_join_graph
            , subq_24.create_a_cycle_in_the_join_graph__listing
            , subq_24.create_a_cycle_in_the_join_graph__guest
            , subq_24.create_a_cycle_in_the_join_graph__host
            , subq_24.is_instant
            , subq_24.create_a_cycle_in_the_join_graph__is_instant
            , subq_24.bookings
            , subq_24.instant_bookings
            , subq_24.booking_value
            , subq_24.max_booking_value
            , subq_24.min_booking_value
            , subq_24.bookers
            , subq_24.average_booking_value
            , subq_24.referred_bookings
            , subq_24.median_booking_value
            , subq_24.booking_value_p99
            , subq_24.discrete_booking_value_p99
            , subq_24.approximate_continuous_booking_value_p99
            , subq_24.approximate_discrete_booking_value_p99
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
          ) subq_24
        ) subq_25
      ) subq_26
      GROUP BY
        subq_26.metric_time
    ) subq_27
  ) subq_28
  ON
    (
      subq_11.metric_time = subq_28.metric_time
    ) OR (
      (subq_11.metric_time IS NULL) AND (subq_28.metric_time IS NULL)
    )
) subq_29
