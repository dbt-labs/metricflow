-- Compute Metrics via Expressions
SELECT
  subq_20.ds
  , subq_20.listing__country_latest
  , CAST(subq_20.bookings AS FLOAT64) / CAST(NULLIF(subq_20.views, 0) AS FLOAT64) AS bookings_per_view
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_9.ds, subq_19.ds) AS ds
    , COALESCE(subq_9.listing__country_latest, subq_19.listing__country_latest) AS listing__country_latest
    , subq_9.bookings AS bookings
    , subq_19.views AS views
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_8.ds
      , subq_8.listing__country_latest
      , subq_8.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_7.ds
        , subq_7.listing__country_latest
        , SUM(subq_7.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'listing__country_latest', 'ds']
        SELECT
          subq_6.ds
          , subq_6.listing__country_latest
          , subq_6.bookings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.ds AS ds
            , subq_2.listing AS listing
            , subq_5.country_latest AS listing__country_latest
            , subq_2.bookings AS bookings
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'ds', 'listing']
            SELECT
              subq_1.ds
              , subq_1.listing
              , subq_1.bookings
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
                  , DATE_TRUNC(bookings_source_src_10001.ds, isoweek) AS ds__week
                  , DATE_TRUNC(bookings_source_src_10001.ds, month) AS ds__month
                  , DATE_TRUNC(bookings_source_src_10001.ds, quarter) AS ds__quarter
                  , DATE_TRUNC(bookings_source_src_10001.ds, isoyear) AS ds__year
                  , bookings_source_src_10001.ds_partitioned
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoweek) AS ds_partitioned__week
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, month) AS ds_partitioned__month
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, quarter) AS ds_partitioned__quarter
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoyear) AS ds_partitioned__year
                  , bookings_source_src_10001.booking_paid_at
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, isoweek) AS booking_paid_at__week
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, month) AS booking_paid_at__month
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, quarter) AS booking_paid_at__quarter
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, isoyear) AS booking_paid_at__year
                  , bookings_source_src_10001.is_instant AS create_a_cycle_in_the_join_graph__is_instant
                  , bookings_source_src_10001.ds AS create_a_cycle_in_the_join_graph__ds
                  , DATE_TRUNC(bookings_source_src_10001.ds, isoweek) AS create_a_cycle_in_the_join_graph__ds__week
                  , DATE_TRUNC(bookings_source_src_10001.ds, month) AS create_a_cycle_in_the_join_graph__ds__month
                  , DATE_TRUNC(bookings_source_src_10001.ds, quarter) AS create_a_cycle_in_the_join_graph__ds__quarter
                  , DATE_TRUNC(bookings_source_src_10001.ds, isoyear) AS create_a_cycle_in_the_join_graph__ds__year
                  , bookings_source_src_10001.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoweek) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, month) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, quarter) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                  , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoyear) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                  , bookings_source_src_10001.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, isoweek) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, month) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, quarter) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
                  , DATE_TRUNC(bookings_source_src_10001.booking_paid_at, isoyear) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
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
            --   ['country_latest', 'listing']
            SELECT
              subq_4.listing
              , subq_4.country_latest
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
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS ds__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS ds__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS ds__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS ds__year
                  , listings_latest_src_10004.created_at
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS created_at__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS created_at__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS created_at__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS created_at__year
                  , listings_latest_src_10004.country AS country_latest
                  , listings_latest_src_10004.is_lux AS is_lux_latest
                  , listings_latest_src_10004.capacity AS capacity_latest
                  , listings_latest_src_10004.created_at AS listing__ds
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__ds__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__ds__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__ds__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS listing__ds__year
                  , listings_latest_src_10004.created_at AS listing__created_at
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__created_at__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__created_at__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__created_at__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS listing__created_at__year
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
      GROUP BY
        ds
        , listing__country_latest
    ) subq_8
  ) subq_9
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_18.ds
      , subq_18.listing__country_latest
      , subq_18.views
    FROM (
      -- Aggregate Measures
      SELECT
        subq_17.ds
        , subq_17.listing__country_latest
        , SUM(subq_17.views) AS views
      FROM (
        -- Pass Only Elements:
        --   ['views', 'listing__country_latest', 'ds']
        SELECT
          subq_16.ds
          , subq_16.listing__country_latest
          , subq_16.views
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_12.ds AS ds
            , subq_12.listing AS listing
            , subq_15.country_latest AS listing__country_latest
            , subq_12.views AS views
          FROM (
            -- Pass Only Elements:
            --   ['views', 'ds', 'listing']
            SELECT
              subq_11.ds
              , subq_11.listing
              , subq_11.views
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_10.ds
                , subq_10.ds__week
                , subq_10.ds__month
                , subq_10.ds__quarter
                , subq_10.ds__year
                , subq_10.ds_partitioned
                , subq_10.ds_partitioned__week
                , subq_10.ds_partitioned__month
                , subq_10.ds_partitioned__quarter
                , subq_10.ds_partitioned__year
                , subq_10.create_a_cycle_in_the_join_graph__ds
                , subq_10.create_a_cycle_in_the_join_graph__ds__week
                , subq_10.create_a_cycle_in_the_join_graph__ds__month
                , subq_10.create_a_cycle_in_the_join_graph__ds__quarter
                , subq_10.create_a_cycle_in_the_join_graph__ds__year
                , subq_10.create_a_cycle_in_the_join_graph__ds_partitioned
                , subq_10.create_a_cycle_in_the_join_graph__ds_partitioned__week
                , subq_10.create_a_cycle_in_the_join_graph__ds_partitioned__month
                , subq_10.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                , subq_10.create_a_cycle_in_the_join_graph__ds_partitioned__year
                , subq_10.ds AS metric_time
                , subq_10.ds__week AS metric_time__week
                , subq_10.ds__month AS metric_time__month
                , subq_10.ds__quarter AS metric_time__quarter
                , subq_10.ds__year AS metric_time__year
                , subq_10.listing
                , subq_10.user
                , subq_10.create_a_cycle_in_the_join_graph
                , subq_10.create_a_cycle_in_the_join_graph__listing
                , subq_10.create_a_cycle_in_the_join_graph__user
                , subq_10.views
              FROM (
                -- Read Elements From Semantic Model 'views_source'
                SELECT
                  1 AS views
                  , views_source_src_10009.ds
                  , DATE_TRUNC(views_source_src_10009.ds, isoweek) AS ds__week
                  , DATE_TRUNC(views_source_src_10009.ds, month) AS ds__month
                  , DATE_TRUNC(views_source_src_10009.ds, quarter) AS ds__quarter
                  , DATE_TRUNC(views_source_src_10009.ds, isoyear) AS ds__year
                  , views_source_src_10009.ds_partitioned
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, isoweek) AS ds_partitioned__week
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, month) AS ds_partitioned__month
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, quarter) AS ds_partitioned__quarter
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, isoyear) AS ds_partitioned__year
                  , views_source_src_10009.ds AS create_a_cycle_in_the_join_graph__ds
                  , DATE_TRUNC(views_source_src_10009.ds, isoweek) AS create_a_cycle_in_the_join_graph__ds__week
                  , DATE_TRUNC(views_source_src_10009.ds, month) AS create_a_cycle_in_the_join_graph__ds__month
                  , DATE_TRUNC(views_source_src_10009.ds, quarter) AS create_a_cycle_in_the_join_graph__ds__quarter
                  , DATE_TRUNC(views_source_src_10009.ds, isoyear) AS create_a_cycle_in_the_join_graph__ds__year
                  , views_source_src_10009.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, isoweek) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, month) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, quarter) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                  , DATE_TRUNC(views_source_src_10009.ds_partitioned, isoyear) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                  , views_source_src_10009.listing_id AS listing
                  , views_source_src_10009.user_id AS user
                  , views_source_src_10009.user_id AS create_a_cycle_in_the_join_graph
                  , views_source_src_10009.listing_id AS create_a_cycle_in_the_join_graph__listing
                  , views_source_src_10009.user_id AS create_a_cycle_in_the_join_graph__user
                FROM ***************************.fct_views views_source_src_10009
              ) subq_10
            ) subq_11
          ) subq_12
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['country_latest', 'listing']
            SELECT
              subq_14.listing
              , subq_14.country_latest
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_13.ds
                , subq_13.ds__week
                , subq_13.ds__month
                , subq_13.ds__quarter
                , subq_13.ds__year
                , subq_13.created_at
                , subq_13.created_at__week
                , subq_13.created_at__month
                , subq_13.created_at__quarter
                , subq_13.created_at__year
                , subq_13.listing__ds
                , subq_13.listing__ds__week
                , subq_13.listing__ds__month
                , subq_13.listing__ds__quarter
                , subq_13.listing__ds__year
                , subq_13.listing__created_at
                , subq_13.listing__created_at__week
                , subq_13.listing__created_at__month
                , subq_13.listing__created_at__quarter
                , subq_13.listing__created_at__year
                , subq_13.ds AS metric_time
                , subq_13.ds__week AS metric_time__week
                , subq_13.ds__month AS metric_time__month
                , subq_13.ds__quarter AS metric_time__quarter
                , subq_13.ds__year AS metric_time__year
                , subq_13.listing
                , subq_13.user
                , subq_13.listing__user
                , subq_13.country_latest
                , subq_13.is_lux_latest
                , subq_13.capacity_latest
                , subq_13.listing__country_latest
                , subq_13.listing__is_lux_latest
                , subq_13.listing__capacity_latest
                , subq_13.listings
                , subq_13.largest_listing
                , subq_13.smallest_listing
              FROM (
                -- Read Elements From Semantic Model 'listings_latest'
                SELECT
                  1 AS listings
                  , listings_latest_src_10004.capacity AS largest_listing
                  , listings_latest_src_10004.capacity AS smallest_listing
                  , listings_latest_src_10004.created_at AS ds
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS ds__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS ds__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS ds__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS ds__year
                  , listings_latest_src_10004.created_at
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS created_at__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS created_at__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS created_at__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS created_at__year
                  , listings_latest_src_10004.country AS country_latest
                  , listings_latest_src_10004.is_lux AS is_lux_latest
                  , listings_latest_src_10004.capacity AS capacity_latest
                  , listings_latest_src_10004.created_at AS listing__ds
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__ds__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__ds__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__ds__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS listing__ds__year
                  , listings_latest_src_10004.created_at AS listing__created_at
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__created_at__week
                  , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__created_at__month
                  , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__created_at__quarter
                  , DATE_TRUNC(listings_latest_src_10004.created_at, isoyear) AS listing__created_at__year
                  , listings_latest_src_10004.country AS listing__country_latest
                  , listings_latest_src_10004.is_lux AS listing__is_lux_latest
                  , listings_latest_src_10004.capacity AS listing__capacity_latest
                  , listings_latest_src_10004.listing_id AS listing
                  , listings_latest_src_10004.user_id AS user
                  , listings_latest_src_10004.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_10004
              ) subq_13
            ) subq_14
          ) subq_15
          ON
            subq_12.listing = subq_15.listing
        ) subq_16
      ) subq_17
      GROUP BY
        ds
        , listing__country_latest
    ) subq_18
  ) subq_19
  ON
    (
      (
        subq_9.listing__country_latest = subq_19.listing__country_latest
      ) OR (
        (
          subq_9.listing__country_latest IS NULL
        ) AND (
          subq_19.listing__country_latest IS NULL
        )
      )
    ) AND (
      (
        subq_9.ds = subq_19.ds
      ) OR (
        (subq_9.ds IS NULL) AND (subq_19.ds IS NULL)
      )
    )
) subq_20
