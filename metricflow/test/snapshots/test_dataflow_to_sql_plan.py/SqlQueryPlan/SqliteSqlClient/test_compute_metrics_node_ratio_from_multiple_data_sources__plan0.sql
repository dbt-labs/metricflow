-- Compute Metrics via Expressions
SELECT
  CAST(subq_19.bookings AS DOUBLE) / CAST(NULLIF(subq_19.views, 0) AS DOUBLE) AS bookings_per_view
  , subq_19.listing__country_latest
  , subq_19.ds
FROM (
  -- Pass Only Elements:
  --   ['listing__country_latest', 'ds', 'bookings', 'views']
  SELECT
    subq_18.bookings
    , subq_18.views
    , subq_18.listing__country_latest
    , subq_18.ds
  FROM (
    -- Join Aggregated Measures with Standard Outputs
    SELECT
      subq_8.bookings AS bookings
      , subq_17.views AS views
      , subq_8.listing__country_latest AS listing__country_latest
      , subq_8.ds AS ds
    FROM (
      -- Aggregate Measures
      SELECT
        SUM(subq_7.bookings) AS bookings
        , subq_7.listing__country_latest
        , subq_7.ds
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'listing__country_latest', 'ds']
        SELECT
          subq_6.bookings
          , subq_6.listing__country_latest
          , subq_6.ds
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.bookings AS bookings
            , subq_5.country_latest AS listing__country_latest
            , subq_2.ds AS ds
            , subq_2.listing AS listing
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'ds', 'listing']
            SELECT
              subq_1.bookings
              , subq_1.ds
              , subq_1.listing
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_0.bookings
                , subq_0.instant_bookings
                , subq_0.booking_value
                , subq_0.max_booking_value
                , subq_0.min_booking_value
                , subq_0.bookers
                , subq_0.average_booking_value
                , subq_0.is_instant
                , subq_0.create_a_cycle_in_the_join_graph__is_instant
                , subq_0.ds
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
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
                  , bookings_source_src_10000.ds_partitioned
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
                  , bookings_source_src_10000.booking_paid_at
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS booking_paid_at__year
                  , bookings_source_src_10000.is_instant AS create_a_cycle_in_the_join_graph__is_instant
                  , bookings_source_src_10000.ds AS create_a_cycle_in_the_join_graph__ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__year
                  , bookings_source_src_10000.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                  , bookings_source_src_10000.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__booking_paid_at__year
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
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['listing', 'country_latest']
            SELECT
              subq_4.country_latest
              , subq_4.listing
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_3.listings
                , subq_3.largest_listing
                , subq_3.smallest_listing
                , subq_3.country_latest
                , subq_3.is_lux_latest
                , subq_3.capacity_latest
                , subq_3.listing__country_latest
                , subq_3.listing__is_lux_latest
                , subq_3.listing__capacity_latest
                , subq_3.ds
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
              FROM (
                -- Read Elements From Data Source 'listings_latest'
                SELECT
                  1 AS listings
                  , listings_latest_src_10003.capacity AS largest_listing
                  , listings_latest_src_10003.capacity AS smallest_listing
                  , listings_latest_src_10003.created_at AS ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
                  , listings_latest_src_10003.created_at
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__year
                  , listings_latest_src_10003.country AS country_latest
                  , listings_latest_src_10003.is_lux AS is_lux_latest
                  , listings_latest_src_10003.capacity AS capacity_latest
                  , listings_latest_src_10003.created_at AS listing__ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__year
                  , listings_latest_src_10003.created_at AS listing__created_at
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__year
                  , listings_latest_src_10003.country AS listing__country_latest
                  , listings_latest_src_10003.is_lux AS listing__is_lux_latest
                  , listings_latest_src_10003.capacity AS listing__capacity_latest
                  , listings_latest_src_10003.listing_id AS listing
                  , listings_latest_src_10003.user_id AS user
                  , listings_latest_src_10003.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_10003
              ) subq_3
            ) subq_4
          ) subq_5
          ON
            subq_2.listing = subq_5.listing
        ) subq_6
      ) subq_7
      GROUP BY
        subq_7.listing__country_latest
        , subq_7.ds
    ) subq_8
    INNER JOIN (
      -- Aggregate Measures
      SELECT
        SUM(subq_16.views) AS views
        , subq_16.listing__country_latest
        , subq_16.ds
      FROM (
        -- Pass Only Elements:
        --   ['views', 'listing__country_latest', 'ds']
        SELECT
          subq_15.views
          , subq_15.listing__country_latest
          , subq_15.ds
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_11.views AS views
            , subq_14.country_latest AS listing__country_latest
            , subq_11.ds AS ds
            , subq_11.listing AS listing
          FROM (
            -- Pass Only Elements:
            --   ['views', 'ds', 'listing']
            SELECT
              subq_10.views
              , subq_10.ds
              , subq_10.listing
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_9.views
                , subq_9.ds
                , subq_9.ds__week
                , subq_9.ds__month
                , subq_9.ds__quarter
                , subq_9.ds__year
                , subq_9.ds_partitioned
                , subq_9.ds_partitioned__week
                , subq_9.ds_partitioned__month
                , subq_9.ds_partitioned__quarter
                , subq_9.ds_partitioned__year
                , subq_9.create_a_cycle_in_the_join_graph__ds
                , subq_9.create_a_cycle_in_the_join_graph__ds__week
                , subq_9.create_a_cycle_in_the_join_graph__ds__month
                , subq_9.create_a_cycle_in_the_join_graph__ds__quarter
                , subq_9.create_a_cycle_in_the_join_graph__ds__year
                , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned
                , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__week
                , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__month
                , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__year
                , subq_9.ds AS metric_time
                , subq_9.ds__week AS metric_time__week
                , subq_9.ds__month AS metric_time__month
                , subq_9.ds__quarter AS metric_time__quarter
                , subq_9.ds__year AS metric_time__year
                , subq_9.listing
                , subq_9.user
                , subq_9.create_a_cycle_in_the_join_graph
                , subq_9.create_a_cycle_in_the_join_graph__listing
                , subq_9.create_a_cycle_in_the_join_graph__user
              FROM (
                -- Read Elements From Data Source 'views_source'
                SELECT
                  1 AS views
                  , views_source_src_10008.ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
                  , views_source_src_10008.ds_partitioned
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
                  , views_source_src_10008.ds AS create_a_cycle_in_the_join_graph__ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds__year
                  , views_source_src_10008.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                  , views_source_src_10008.listing_id AS listing
                  , views_source_src_10008.user_id AS user
                  , views_source_src_10008.user_id AS create_a_cycle_in_the_join_graph
                  , views_source_src_10008.listing_id AS create_a_cycle_in_the_join_graph__listing
                  , views_source_src_10008.user_id AS create_a_cycle_in_the_join_graph__user
                FROM (
                  -- User Defined SQL Query
                  SELECT user_id, listing_id, ds, ds_partitioned FROM ***************************.fct_views
                ) views_source_src_10008
              ) subq_9
            ) subq_10
          ) subq_11
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['listing', 'country_latest']
            SELECT
              subq_13.country_latest
              , subq_13.listing
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_12.listings
                , subq_12.largest_listing
                , subq_12.smallest_listing
                , subq_12.country_latest
                , subq_12.is_lux_latest
                , subq_12.capacity_latest
                , subq_12.listing__country_latest
                , subq_12.listing__is_lux_latest
                , subq_12.listing__capacity_latest
                , subq_12.ds
                , subq_12.ds__week
                , subq_12.ds__month
                , subq_12.ds__quarter
                , subq_12.ds__year
                , subq_12.created_at
                , subq_12.created_at__week
                , subq_12.created_at__month
                , subq_12.created_at__quarter
                , subq_12.created_at__year
                , subq_12.listing__ds
                , subq_12.listing__ds__week
                , subq_12.listing__ds__month
                , subq_12.listing__ds__quarter
                , subq_12.listing__ds__year
                , subq_12.listing__created_at
                , subq_12.listing__created_at__week
                , subq_12.listing__created_at__month
                , subq_12.listing__created_at__quarter
                , subq_12.listing__created_at__year
                , subq_12.ds AS metric_time
                , subq_12.ds__week AS metric_time__week
                , subq_12.ds__month AS metric_time__month
                , subq_12.ds__quarter AS metric_time__quarter
                , subq_12.ds__year AS metric_time__year
                , subq_12.listing
                , subq_12.user
                , subq_12.listing__user
              FROM (
                -- Read Elements From Data Source 'listings_latest'
                SELECT
                  1 AS listings
                  , listings_latest_src_10003.capacity AS largest_listing
                  , listings_latest_src_10003.capacity AS smallest_listing
                  , listings_latest_src_10003.created_at AS ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
                  , listings_latest_src_10003.created_at
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS created_at__year
                  , listings_latest_src_10003.country AS country_latest
                  , listings_latest_src_10003.is_lux AS is_lux_latest
                  , listings_latest_src_10003.capacity AS capacity_latest
                  , listings_latest_src_10003.created_at AS listing__ds
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__ds__year
                  , listings_latest_src_10003.created_at AS listing__created_at
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__week
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__month
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__quarter
                  , '__DATE_TRUNC_NOT_SUPPORTED__' AS listing__created_at__year
                  , listings_latest_src_10003.country AS listing__country_latest
                  , listings_latest_src_10003.is_lux AS listing__is_lux_latest
                  , listings_latest_src_10003.capacity AS listing__capacity_latest
                  , listings_latest_src_10003.listing_id AS listing
                  , listings_latest_src_10003.user_id AS user
                  , listings_latest_src_10003.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_10003
              ) subq_12
            ) subq_13
          ) subq_14
          ON
            subq_11.listing = subq_14.listing
        ) subq_15
      ) subq_16
      GROUP BY
        subq_16.listing__country_latest
        , subq_16.ds
    ) subq_17
    ON
      (
        (
          subq_8.listing__country_latest = subq_17.listing__country_latest
        ) OR (
          (
            subq_8.listing__country_latest IS NULL
          ) AND (
            subq_17.listing__country_latest IS NULL
          )
        )
      ) AND (
        (
          subq_8.ds = subq_17.ds
        ) OR (
          (subq_8.ds IS NULL) AND (subq_17.ds IS NULL)
        )
      )
  ) subq_18
) subq_19
