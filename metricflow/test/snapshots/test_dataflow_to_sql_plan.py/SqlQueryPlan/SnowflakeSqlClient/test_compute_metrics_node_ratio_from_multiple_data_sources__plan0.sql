-- Compute Metrics via Expressions
SELECT
  CAST(subq_15.bookings AS DOUBLE) / CAST(NULLIF(subq_15.views, 0) AS DOUBLE) AS bookings_per_view
  , subq_15.listing__country_latest
  , subq_15.ds
FROM (
  -- Pass Only Elements:
  --   ['listing__country_latest', 'ds', 'bookings', 'views']
  SELECT
    subq_14.bookings
    , subq_14.views
    , subq_14.listing__country_latest
    , subq_14.ds
  FROM (
    -- Join Aggregated Measures with Standard Outputs
    SELECT
      subq_6.bookings AS bookings
      , subq_13.views AS views
      , subq_6.listing__country_latest AS listing__country_latest
      , subq_6.ds AS ds
    FROM (
      -- Aggregate Measures
      SELECT
        SUM(subq_5.bookings) AS bookings
        , subq_5.listing__country_latest
        , subq_5.ds
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'listing__country_latest', 'ds']
        SELECT
          subq_4.bookings
          , subq_4.listing__country_latest
          , subq_4.ds
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_1.bookings AS bookings
            , subq_3.country_latest AS listing__country_latest
            , subq_1.ds AS ds
            , subq_1.listing AS listing
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'ds', 'listing']
            SELECT
              subq_0.bookings
              , subq_0.ds
              , subq_0.listing
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
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['listing', 'country_latest']
            SELECT
              subq_2.country_latest
              , subq_2.listing
            FROM (
              -- Read Elements From Data Source 'listings_latest'
              SELECT
                1 AS listings
                , listings_latest_src_10003.capacity AS largest_listing
                , listings_latest_src_10003.capacity AS smallest_listing
                , listings_latest_src_10003.created_at AS ds
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS ds__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS ds__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS ds__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS ds__year
                , listings_latest_src_10003.created_at
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS created_at__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS created_at__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS created_at__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS created_at__year
                , listings_latest_src_10003.country AS country_latest
                , listings_latest_src_10003.is_lux AS is_lux_latest
                , listings_latest_src_10003.capacity AS capacity_latest
                , listings_latest_src_10003.created_at AS listing__ds
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS listing__ds__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS listing__ds__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS listing__ds__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS listing__ds__year
                , listings_latest_src_10003.created_at AS listing__created_at
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS listing__created_at__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS listing__created_at__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS listing__created_at__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS listing__created_at__year
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
      ) subq_5
      GROUP BY
        subq_5.listing__country_latest
        , subq_5.ds
    ) subq_6
    INNER JOIN (
      -- Aggregate Measures
      SELECT
        SUM(subq_12.views) AS views
        , subq_12.listing__country_latest
        , subq_12.ds
      FROM (
        -- Pass Only Elements:
        --   ['views', 'listing__country_latest', 'ds']
        SELECT
          subq_11.views
          , subq_11.listing__country_latest
          , subq_11.ds
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_8.views AS views
            , subq_10.country_latest AS listing__country_latest
            , subq_8.ds AS ds
            , subq_8.listing AS listing
          FROM (
            -- Pass Only Elements:
            --   ['views', 'ds', 'listing']
            SELECT
              subq_7.views
              , subq_7.ds
              , subq_7.listing
            FROM (
              -- Read Elements From Data Source 'views_source'
              SELECT
                1 AS views
                , views_source_src_10008.ds
                , DATE_TRUNC('week', views_source_src_10008.ds) AS ds__week
                , DATE_TRUNC('month', views_source_src_10008.ds) AS ds__month
                , DATE_TRUNC('quarter', views_source_src_10008.ds) AS ds__quarter
                , DATE_TRUNC('year', views_source_src_10008.ds) AS ds__year
                , views_source_src_10008.ds_partitioned
                , DATE_TRUNC('week', views_source_src_10008.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', views_source_src_10008.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', views_source_src_10008.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', views_source_src_10008.ds_partitioned) AS ds_partitioned__year
                , views_source_src_10008.ds AS create_a_cycle_in_the_join_graph__ds
                , DATE_TRUNC('week', views_source_src_10008.ds) AS create_a_cycle_in_the_join_graph__ds__week
                , DATE_TRUNC('month', views_source_src_10008.ds) AS create_a_cycle_in_the_join_graph__ds__month
                , DATE_TRUNC('quarter', views_source_src_10008.ds) AS create_a_cycle_in_the_join_graph__ds__quarter
                , DATE_TRUNC('year', views_source_src_10008.ds) AS create_a_cycle_in_the_join_graph__ds__year
                , views_source_src_10008.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                , DATE_TRUNC('week', views_source_src_10008.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                , DATE_TRUNC('month', views_source_src_10008.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                , DATE_TRUNC('quarter', views_source_src_10008.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                , DATE_TRUNC('year', views_source_src_10008.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                , views_source_src_10008.listing_id AS listing
                , views_source_src_10008.user_id AS user
                , views_source_src_10008.user_id AS create_a_cycle_in_the_join_graph
                , views_source_src_10008.listing_id AS create_a_cycle_in_the_join_graph__listing
                , views_source_src_10008.user_id AS create_a_cycle_in_the_join_graph__user
              FROM (
                -- User Defined SQL Query
                SELECT user_id, listing_id, ds, ds_partitioned FROM ***************************.fct_views
              ) views_source_src_10008
            ) subq_7
          ) subq_8
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['listing', 'country_latest']
            SELECT
              subq_9.country_latest
              , subq_9.listing
            FROM (
              -- Read Elements From Data Source 'listings_latest'
              SELECT
                1 AS listings
                , listings_latest_src_10003.capacity AS largest_listing
                , listings_latest_src_10003.capacity AS smallest_listing
                , listings_latest_src_10003.created_at AS ds
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS ds__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS ds__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS ds__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS ds__year
                , listings_latest_src_10003.created_at
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS created_at__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS created_at__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS created_at__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS created_at__year
                , listings_latest_src_10003.country AS country_latest
                , listings_latest_src_10003.is_lux AS is_lux_latest
                , listings_latest_src_10003.capacity AS capacity_latest
                , listings_latest_src_10003.created_at AS listing__ds
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS listing__ds__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS listing__ds__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS listing__ds__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS listing__ds__year
                , listings_latest_src_10003.created_at AS listing__created_at
                , DATE_TRUNC('week', listings_latest_src_10003.created_at) AS listing__created_at__week
                , DATE_TRUNC('month', listings_latest_src_10003.created_at) AS listing__created_at__month
                , DATE_TRUNC('quarter', listings_latest_src_10003.created_at) AS listing__created_at__quarter
                , DATE_TRUNC('year', listings_latest_src_10003.created_at) AS listing__created_at__year
                , listings_latest_src_10003.country AS listing__country_latest
                , listings_latest_src_10003.is_lux AS listing__is_lux_latest
                , listings_latest_src_10003.capacity AS listing__capacity_latest
                , listings_latest_src_10003.listing_id AS listing
                , listings_latest_src_10003.user_id AS user
                , listings_latest_src_10003.user_id AS listing__user
              FROM ***************************.dim_listings_latest listings_latest_src_10003
            ) subq_9
          ) subq_10
          ON
            subq_8.listing = subq_10.listing
        ) subq_11
      ) subq_12
      GROUP BY
        subq_12.listing__country_latest
        , subq_12.ds
    ) subq_13
    ON
      (
        (
          subq_6.listing__country_latest = subq_13.listing__country_latest
        ) OR (
          (
            subq_6.listing__country_latest IS NULL
          ) AND (
            subq_13.listing__country_latest IS NULL
          )
        )
      ) AND (
        (
          subq_6.ds = subq_13.ds
        ) OR (
          (subq_6.ds IS NULL) AND (subq_13.ds IS NULL)
        )
      )
  ) subq_14
) subq_15
