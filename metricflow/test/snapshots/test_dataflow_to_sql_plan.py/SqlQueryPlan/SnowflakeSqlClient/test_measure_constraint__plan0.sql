-- Compute Metrics via Expressions
SELECT
  subq_19.metric_time
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Pass Only Elements:
  --   ['metric_time', 'average_booking_value', 'bookings', 'booking_value']
  SELECT
    subq_18.metric_time
    , subq_18.bookings
    , subq_18.average_booking_value
    , subq_18.booking_value
  FROM (
    -- Join Aggregated Measures with Standard Outputs
    SELECT
      subq_12.metric_time AS metric_time
      , subq_12.bookings AS bookings
      , subq_12.average_booking_value AS average_booking_value
      , subq_17.booking_value AS booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_11.metric_time
        , SUM(subq_11.bookings) AS bookings
        , AVG(subq_11.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements:
        --   ['average_booking_value', 'bookings', 'metric_time']
        SELECT
          subq_10.metric_time
          , subq_10.bookings
          , subq_10.average_booking_value
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_9.metric_time
            , subq_9.listing__is_lux_latest
            , subq_9.bookings
            , subq_9.average_booking_value
          FROM (
            -- Pass Only Elements:
            --   ['average_booking_value', 'bookings', 'listing__is_lux_latest', 'metric_time']
            SELECT
              subq_8.metric_time
              , subq_8.listing__is_lux_latest
              , subq_8.bookings
              , subq_8.average_booking_value
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_3.metric_time AS metric_time
                , subq_3.listing AS listing
                , subq_7.is_lux_latest AS listing__is_lux_latest
                , subq_3.bookings AS bookings
                , subq_3.average_booking_value AS average_booking_value
              FROM (
                -- Pass Only Elements:
                --   ['average_booking_value', 'bookings', 'metric_time', 'listing']
                SELECT
                  subq_2.metric_time
                  , subq_2.listing
                  , subq_2.bookings
                  , subq_2.average_booking_value
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_1.ds AS metric_time
                    , subq_1.ds__week AS metric_time__week
                    , subq_1.ds__month AS metric_time__month
                    , subq_1.ds__quarter AS metric_time__quarter
                    , subq_1.ds__year AS metric_time__year
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
                  FROM (
                    -- Pass Only Additive Measures
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
                        , bookings_source_src_10001.booking_value
                        , bookings_source_src_10001.booking_value AS max_booking_value
                        , bookings_source_src_10001.booking_value AS min_booking_value
                        , bookings_source_src_10001.guest_id AS bookers
                        , bookings_source_src_10001.booking_value AS average_booking_value
                        , bookings_source_src_10001.booking_value AS booking_payments
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
                      FROM (
                        -- User Defined SQL Query
                        SELECT * FROM ***************************.fct_bookings
                      ) bookings_source_src_10001
                    ) subq_0
                  ) subq_1
                ) subq_2
              ) subq_3
              LEFT OUTER JOIN (
                -- Pass Only Elements:
                --   ['listing', 'is_lux_latest']
                SELECT
                  subq_6.listing
                  , subq_6.is_lux_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_5.ds
                    , subq_5.ds__week
                    , subq_5.ds__month
                    , subq_5.ds__quarter
                    , subq_5.ds__year
                    , subq_5.created_at
                    , subq_5.created_at__week
                    , subq_5.created_at__month
                    , subq_5.created_at__quarter
                    , subq_5.created_at__year
                    , subq_5.listing__ds
                    , subq_5.listing__ds__week
                    , subq_5.listing__ds__month
                    , subq_5.listing__ds__quarter
                    , subq_5.listing__ds__year
                    , subq_5.listing__created_at
                    , subq_5.listing__created_at__week
                    , subq_5.listing__created_at__month
                    , subq_5.listing__created_at__quarter
                    , subq_5.listing__created_at__year
                    , subq_5.ds AS metric_time
                    , subq_5.ds__week AS metric_time__week
                    , subq_5.ds__month AS metric_time__month
                    , subq_5.ds__quarter AS metric_time__quarter
                    , subq_5.ds__year AS metric_time__year
                    , subq_5.listing
                    , subq_5.user
                    , subq_5.listing__user
                    , subq_5.country_latest
                    , subq_5.is_lux_latest
                    , subq_5.capacity_latest
                    , subq_5.listing__country_latest
                    , subq_5.listing__is_lux_latest
                    , subq_5.listing__capacity_latest
                    , subq_5.listings
                    , subq_5.largest_listing
                    , subq_5.smallest_listing
                  FROM (
                    -- Pass Only Additive Measures
                    SELECT
                      subq_4.ds
                      , subq_4.ds__week
                      , subq_4.ds__month
                      , subq_4.ds__quarter
                      , subq_4.ds__year
                      , subq_4.created_at
                      , subq_4.created_at__week
                      , subq_4.created_at__month
                      , subq_4.created_at__quarter
                      , subq_4.created_at__year
                      , subq_4.listing__ds
                      , subq_4.listing__ds__week
                      , subq_4.listing__ds__month
                      , subq_4.listing__ds__quarter
                      , subq_4.listing__ds__year
                      , subq_4.listing__created_at
                      , subq_4.listing__created_at__week
                      , subq_4.listing__created_at__month
                      , subq_4.listing__created_at__quarter
                      , subq_4.listing__created_at__year
                      , subq_4.listing
                      , subq_4.user
                      , subq_4.listing__user
                      , subq_4.country_latest
                      , subq_4.is_lux_latest
                      , subq_4.capacity_latest
                      , subq_4.listing__country_latest
                      , subq_4.listing__is_lux_latest
                      , subq_4.listing__capacity_latest
                      , subq_4.listings
                      , subq_4.largest_listing
                      , subq_4.smallest_listing
                    FROM (
                      -- Read Elements From Data Source 'listings_latest'
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
                    ) subq_4
                  ) subq_5
                ) subq_6
              ) subq_7
              ON
                subq_3.listing = subq_7.listing
            ) subq_8
          ) subq_9
          WHERE listing__is_lux_latest
        ) subq_10
      ) subq_11
      GROUP BY
        subq_11.metric_time
    ) subq_12
    INNER JOIN (
      -- Aggregate Measures
      SELECT
        subq_16.metric_time
        , SUM(subq_16.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time']
        SELECT
          subq_15.metric_time
          , subq_15.booking_value
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_14.ds
            , subq_14.ds__week
            , subq_14.ds__month
            , subq_14.ds__quarter
            , subq_14.ds__year
            , subq_14.ds_partitioned
            , subq_14.ds_partitioned__week
            , subq_14.ds_partitioned__month
            , subq_14.ds_partitioned__quarter
            , subq_14.ds_partitioned__year
            , subq_14.booking_paid_at
            , subq_14.booking_paid_at__week
            , subq_14.booking_paid_at__month
            , subq_14.booking_paid_at__quarter
            , subq_14.booking_paid_at__year
            , subq_14.create_a_cycle_in_the_join_graph__ds
            , subq_14.create_a_cycle_in_the_join_graph__ds__week
            , subq_14.create_a_cycle_in_the_join_graph__ds__month
            , subq_14.create_a_cycle_in_the_join_graph__ds__quarter
            , subq_14.create_a_cycle_in_the_join_graph__ds__year
            , subq_14.create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_14.create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_14.create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_14.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_14.create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_14.create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_14.create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_14.create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_14.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_14.create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_14.ds AS metric_time
            , subq_14.ds__week AS metric_time__week
            , subq_14.ds__month AS metric_time__month
            , subq_14.ds__quarter AS metric_time__quarter
            , subq_14.ds__year AS metric_time__year
            , subq_14.listing
            , subq_14.guest
            , subq_14.host
            , subq_14.create_a_cycle_in_the_join_graph
            , subq_14.create_a_cycle_in_the_join_graph__listing
            , subq_14.create_a_cycle_in_the_join_graph__guest
            , subq_14.create_a_cycle_in_the_join_graph__host
            , subq_14.is_instant
            , subq_14.create_a_cycle_in_the_join_graph__is_instant
            , subq_14.bookings
            , subq_14.instant_bookings
            , subq_14.booking_value
            , subq_14.max_booking_value
            , subq_14.min_booking_value
            , subq_14.bookers
            , subq_14.average_booking_value
          FROM (
            -- Pass Only Additive Measures
            SELECT
              subq_13.ds
              , subq_13.ds__week
              , subq_13.ds__month
              , subq_13.ds__quarter
              , subq_13.ds__year
              , subq_13.ds_partitioned
              , subq_13.ds_partitioned__week
              , subq_13.ds_partitioned__month
              , subq_13.ds_partitioned__quarter
              , subq_13.ds_partitioned__year
              , subq_13.booking_paid_at
              , subq_13.booking_paid_at__week
              , subq_13.booking_paid_at__month
              , subq_13.booking_paid_at__quarter
              , subq_13.booking_paid_at__year
              , subq_13.create_a_cycle_in_the_join_graph__ds
              , subq_13.create_a_cycle_in_the_join_graph__ds__week
              , subq_13.create_a_cycle_in_the_join_graph__ds__month
              , subq_13.create_a_cycle_in_the_join_graph__ds__quarter
              , subq_13.create_a_cycle_in_the_join_graph__ds__year
              , subq_13.create_a_cycle_in_the_join_graph__ds_partitioned
              , subq_13.create_a_cycle_in_the_join_graph__ds_partitioned__week
              , subq_13.create_a_cycle_in_the_join_graph__ds_partitioned__month
              , subq_13.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
              , subq_13.create_a_cycle_in_the_join_graph__ds_partitioned__year
              , subq_13.create_a_cycle_in_the_join_graph__booking_paid_at
              , subq_13.create_a_cycle_in_the_join_graph__booking_paid_at__week
              , subq_13.create_a_cycle_in_the_join_graph__booking_paid_at__month
              , subq_13.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
              , subq_13.create_a_cycle_in_the_join_graph__booking_paid_at__year
              , subq_13.listing
              , subq_13.guest
              , subq_13.host
              , subq_13.create_a_cycle_in_the_join_graph
              , subq_13.create_a_cycle_in_the_join_graph__listing
              , subq_13.create_a_cycle_in_the_join_graph__guest
              , subq_13.create_a_cycle_in_the_join_graph__host
              , subq_13.is_instant
              , subq_13.create_a_cycle_in_the_join_graph__is_instant
              , subq_13.bookings
              , subq_13.instant_bookings
              , subq_13.booking_value
              , subq_13.max_booking_value
              , subq_13.min_booking_value
              , subq_13.bookers
              , subq_13.average_booking_value
            FROM (
              -- Read Elements From Data Source 'bookings_source'
              SELECT
                1 AS bookings
                , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                , bookings_source_src_10001.booking_value
                , bookings_source_src_10001.booking_value AS max_booking_value
                , bookings_source_src_10001.booking_value AS min_booking_value
                , bookings_source_src_10001.guest_id AS bookers
                , bookings_source_src_10001.booking_value AS average_booking_value
                , bookings_source_src_10001.booking_value AS booking_payments
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
              FROM (
                -- User Defined SQL Query
                SELECT * FROM ***************************.fct_bookings
              ) bookings_source_src_10001
            ) subq_13
          ) subq_14
        ) subq_15
      ) subq_16
      GROUP BY
        subq_16.metric_time
    ) subq_17
    ON
      (
        (
          subq_12.metric_time = subq_17.metric_time
        ) OR (
          (subq_12.metric_time IS NULL) AND (subq_17.metric_time IS NULL)
        )
      )
  ) subq_18
) subq_19
