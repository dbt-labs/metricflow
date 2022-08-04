-- Compute Metrics via Expressions
SELECT
  subq_23.ds
  , subq_23.listing__country_latest
  , CAST(subq_23.bookings AS DOUBLE PRECISION) / CAST(NULLIF(subq_23.views, 0) AS DOUBLE PRECISION) AS bookings_per_view
FROM (
  -- Pass Only Elements:
  --   ['listing__country_latest', 'ds', 'bookings', 'views']
  SELECT
    subq_22.ds
    , subq_22.listing__country_latest
    , subq_22.bookings
    , subq_22.views
  FROM (
    -- Join Aggregated Measures with Standard Outputs
    SELECT
      subq_10.ds AS ds
      , subq_10.listing__country_latest AS listing__country_latest
      , subq_10.bookings AS bookings
      , subq_21.views AS views
    FROM (
      -- Aggregate Measures
      SELECT
        subq_9.ds
        , subq_9.listing__country_latest
        , SUM(subq_9.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'listing__country_latest', 'ds']
        SELECT
          subq_8.ds
          , subq_8.listing__country_latest
          , subq_8.bookings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_3.ds AS ds
            , subq_3.listing AS listing
            , subq_7.country_latest AS listing__country_latest
            , subq_3.bookings AS bookings
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'ds', 'listing']
            SELECT
              subq_2.ds
              , subq_2.listing
              , subq_2.bookings
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
            --   ['listing', 'country_latest']
            SELECT
              subq_6.listing
              , subq_6.country_latest
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
      GROUP BY
        subq_9.ds
        , subq_9.listing__country_latest
    ) subq_10
    INNER JOIN (
      -- Aggregate Measures
      SELECT
        subq_20.ds
        , subq_20.listing__country_latest
        , SUM(subq_20.views) AS views
      FROM (
        -- Pass Only Elements:
        --   ['views', 'listing__country_latest', 'ds']
        SELECT
          subq_19.ds
          , subq_19.listing__country_latest
          , subq_19.views
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_14.ds AS ds
            , subq_14.listing AS listing
            , subq_18.country_latest AS listing__country_latest
            , subq_14.views AS views
          FROM (
            -- Pass Only Elements:
            --   ['views', 'ds', 'listing']
            SELECT
              subq_13.ds
              , subq_13.listing
              , subq_13.views
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
                , subq_12.ds AS metric_time
                , subq_12.ds__week AS metric_time__week
                , subq_12.ds__month AS metric_time__month
                , subq_12.ds__quarter AS metric_time__quarter
                , subq_12.ds__year AS metric_time__year
                , subq_12.listing
                , subq_12.user
                , subq_12.create_a_cycle_in_the_join_graph
                , subq_12.create_a_cycle_in_the_join_graph__listing
                , subq_12.create_a_cycle_in_the_join_graph__user
                , subq_12.views
              FROM (
                -- Pass Only Additive Measures
                SELECT
                  subq_11.ds
                  , subq_11.ds__week
                  , subq_11.ds__month
                  , subq_11.ds__quarter
                  , subq_11.ds__year
                  , subq_11.ds_partitioned
                  , subq_11.ds_partitioned__week
                  , subq_11.ds_partitioned__month
                  , subq_11.ds_partitioned__quarter
                  , subq_11.ds_partitioned__year
                  , subq_11.create_a_cycle_in_the_join_graph__ds
                  , subq_11.create_a_cycle_in_the_join_graph__ds__week
                  , subq_11.create_a_cycle_in_the_join_graph__ds__month
                  , subq_11.create_a_cycle_in_the_join_graph__ds__quarter
                  , subq_11.create_a_cycle_in_the_join_graph__ds__year
                  , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned
                  , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__week
                  , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__month
                  , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                  , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__year
                  , subq_11.listing
                  , subq_11.user
                  , subq_11.create_a_cycle_in_the_join_graph
                  , subq_11.create_a_cycle_in_the_join_graph__listing
                  , subq_11.create_a_cycle_in_the_join_graph__user
                  , subq_11.views
                FROM (
                  -- Read Elements From Data Source 'views_source'
                  SELECT
                    1 AS views
                    , views_source_src_10009.ds
                    , DATE_TRUNC('week', views_source_src_10009.ds) AS ds__week
                    , DATE_TRUNC('month', views_source_src_10009.ds) AS ds__month
                    , DATE_TRUNC('quarter', views_source_src_10009.ds) AS ds__quarter
                    , DATE_TRUNC('year', views_source_src_10009.ds) AS ds__year
                    , views_source_src_10009.ds_partitioned
                    , DATE_TRUNC('week', views_source_src_10009.ds_partitioned) AS ds_partitioned__week
                    , DATE_TRUNC('month', views_source_src_10009.ds_partitioned) AS ds_partitioned__month
                    , DATE_TRUNC('quarter', views_source_src_10009.ds_partitioned) AS ds_partitioned__quarter
                    , DATE_TRUNC('year', views_source_src_10009.ds_partitioned) AS ds_partitioned__year
                    , views_source_src_10009.ds AS create_a_cycle_in_the_join_graph__ds
                    , DATE_TRUNC('week', views_source_src_10009.ds) AS create_a_cycle_in_the_join_graph__ds__week
                    , DATE_TRUNC('month', views_source_src_10009.ds) AS create_a_cycle_in_the_join_graph__ds__month
                    , DATE_TRUNC('quarter', views_source_src_10009.ds) AS create_a_cycle_in_the_join_graph__ds__quarter
                    , DATE_TRUNC('year', views_source_src_10009.ds) AS create_a_cycle_in_the_join_graph__ds__year
                    , views_source_src_10009.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                    , DATE_TRUNC('week', views_source_src_10009.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                    , DATE_TRUNC('month', views_source_src_10009.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                    , DATE_TRUNC('quarter', views_source_src_10009.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                    , DATE_TRUNC('year', views_source_src_10009.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                    , views_source_src_10009.listing_id AS listing
                    , views_source_src_10009.user_id AS user
                    , views_source_src_10009.user_id AS create_a_cycle_in_the_join_graph
                    , views_source_src_10009.listing_id AS create_a_cycle_in_the_join_graph__listing
                    , views_source_src_10009.user_id AS create_a_cycle_in_the_join_graph__user
                  FROM (
                    -- User Defined SQL Query
                    SELECT user_id, listing_id, ds, ds_partitioned FROM ***************************.fct_views
                  ) views_source_src_10009
                ) subq_11
              ) subq_12
            ) subq_13
          ) subq_14
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['listing', 'country_latest']
            SELECT
              subq_17.listing
              , subq_17.country_latest
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_16.ds
                , subq_16.ds__week
                , subq_16.ds__month
                , subq_16.ds__quarter
                , subq_16.ds__year
                , subq_16.created_at
                , subq_16.created_at__week
                , subq_16.created_at__month
                , subq_16.created_at__quarter
                , subq_16.created_at__year
                , subq_16.listing__ds
                , subq_16.listing__ds__week
                , subq_16.listing__ds__month
                , subq_16.listing__ds__quarter
                , subq_16.listing__ds__year
                , subq_16.listing__created_at
                , subq_16.listing__created_at__week
                , subq_16.listing__created_at__month
                , subq_16.listing__created_at__quarter
                , subq_16.listing__created_at__year
                , subq_16.ds AS metric_time
                , subq_16.ds__week AS metric_time__week
                , subq_16.ds__month AS metric_time__month
                , subq_16.ds__quarter AS metric_time__quarter
                , subq_16.ds__year AS metric_time__year
                , subq_16.listing
                , subq_16.user
                , subq_16.listing__user
                , subq_16.country_latest
                , subq_16.is_lux_latest
                , subq_16.capacity_latest
                , subq_16.listing__country_latest
                , subq_16.listing__is_lux_latest
                , subq_16.listing__capacity_latest
                , subq_16.listings
                , subq_16.largest_listing
                , subq_16.smallest_listing
              FROM (
                -- Pass Only Additive Measures
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
                ) subq_15
              ) subq_16
            ) subq_17
          ) subq_18
          ON
            subq_14.listing = subq_18.listing
        ) subq_19
      ) subq_20
      GROUP BY
        subq_20.ds
        , subq_20.listing__country_latest
    ) subq_21
    ON
      (
        (
          subq_10.listing__country_latest = subq_21.listing__country_latest
        ) OR (
          (
            subq_10.listing__country_latest IS NULL
          ) AND (
            subq_21.listing__country_latest IS NULL
          )
        )
      ) AND (
        (
          subq_10.ds = subq_21.ds
        ) OR (
          (subq_10.ds IS NULL) AND (subq_21.ds IS NULL)
        )
      )
  ) subq_22
) subq_23
