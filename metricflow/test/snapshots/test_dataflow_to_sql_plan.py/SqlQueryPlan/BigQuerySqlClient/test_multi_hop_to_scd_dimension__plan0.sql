-- Compute Metrics via Expressions
SELECT
  subq_10.metric_time
  , subq_10.listing__lux_listing__is_confirmed_lux
  , subq_10.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_9.metric_time
    , subq_9.listing__lux_listing__is_confirmed_lux
    , SUM(subq_9.bookings) AS bookings
  FROM (
    -- Pass Only Elements:
    --   ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time']
    SELECT
      subq_8.metric_time
      , subq_8.listing__lux_listing__is_confirmed_lux
      , subq_8.bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_2.metric_time AS metric_time
        , subq_7.lux_listing__window_start AS listing__lux_listing__window_start
        , subq_7.lux_listing__window_end AS listing__lux_listing__window_end
        , subq_2.listing AS listing
        , subq_7.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
        , subq_2.bookings AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time', 'listing']
        SELECT
          subq_1.metric_time
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
            , subq_0.ds AS metric_time
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.listing
            , subq_0.guest
            , subq_0.host
            , subq_0.user
            , subq_0.is_instant
            , subq_0.bookings
            , subq_0.instant_bookings
            , subq_0.booking_value
            , subq_0.bookers
            , subq_0.average_booking_value
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            SELECT
              1 AS bookings
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_10015.booking_value
              , bookings_source_src_10015.guest_id AS bookers
              , bookings_source_src_10015.booking_value AS average_booking_value
              , bookings_source_src_10015.booking_value AS booking_payments
              , bookings_source_src_10015.is_instant
              , bookings_source_src_10015.ds
              , DATE_TRUNC(bookings_source_src_10015.ds, isoweek) AS ds__week
              , DATE_TRUNC(bookings_source_src_10015.ds, month) AS ds__month
              , DATE_TRUNC(bookings_source_src_10015.ds, quarter) AS ds__quarter
              , DATE_TRUNC(bookings_source_src_10015.ds, isoyear) AS ds__year
              , bookings_source_src_10015.ds_partitioned
              , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, isoweek) AS ds_partitioned__week
              , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, month) AS ds_partitioned__month
              , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, quarter) AS ds_partitioned__quarter
              , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, isoyear) AS ds_partitioned__year
              , bookings_source_src_10015.booking_paid_at
              , DATE_TRUNC(bookings_source_src_10015.booking_paid_at, isoweek) AS booking_paid_at__week
              , DATE_TRUNC(bookings_source_src_10015.booking_paid_at, month) AS booking_paid_at__month
              , DATE_TRUNC(bookings_source_src_10015.booking_paid_at, quarter) AS booking_paid_at__quarter
              , DATE_TRUNC(bookings_source_src_10015.booking_paid_at, isoyear) AS booking_paid_at__year
              , bookings_source_src_10015.listing_id AS listing
              , bookings_source_src_10015.guest_id AS guest
              , bookings_source_src_10015.host_id AS host
              , bookings_source_src_10015.guest_id AS user
            FROM ***************************.fct_bookings bookings_source_src_10015
          ) subq_0
        ) subq_1
      ) subq_2
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['lux_listing__is_confirmed_lux',
        --    'lux_listing__window_start',
        --    'lux_listing__window_end',
        --    'listing']
        SELECT
          subq_6.lux_listing__window_start
          , subq_6.lux_listing__window_end
          , subq_6.listing
          , subq_6.lux_listing__is_confirmed_lux
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_5.window_start AS lux_listing__window_start
            , subq_5.window_start__week AS lux_listing__window_start__week
            , subq_5.window_start__month AS lux_listing__window_start__month
            , subq_5.window_start__quarter AS lux_listing__window_start__quarter
            , subq_5.window_start__year AS lux_listing__window_start__year
            , subq_5.window_end AS lux_listing__window_end
            , subq_5.window_end__week AS lux_listing__window_end__week
            , subq_5.window_end__month AS lux_listing__window_end__month
            , subq_5.window_end__quarter AS lux_listing__window_end__quarter
            , subq_5.window_end__year AS lux_listing__window_end__year
            , subq_3.listing AS listing
            , subq_3.lux_listing AS lux_listing
            , subq_3.listing__lux_listing AS listing__lux_listing
            , subq_5.is_confirmed_lux AS lux_listing__is_confirmed_lux
          FROM (
            -- Read Elements From Semantic Model 'lux_listing_mapping'
            SELECT
              lux_listing_mapping_src_10018.listing_id AS listing
              , lux_listing_mapping_src_10018.lux_listing_id AS lux_listing
              , lux_listing_mapping_src_10018.lux_listing_id AS listing__lux_listing
            FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_10018
          ) subq_3
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['is_confirmed_lux',
            --    'lux_listing__is_confirmed_lux',
            --    'window_start',
            --    'window_start__week',
            --    'window_start__month',
            --    'window_start__quarter',
            --    'window_start__year',
            --    'window_end',
            --    'window_end__week',
            --    'window_end__month',
            --    'window_end__quarter',
            --    'window_end__year',
            --    'lux_listing__window_start',
            --    'lux_listing__window_start__week',
            --    'lux_listing__window_start__month',
            --    'lux_listing__window_start__quarter',
            --    'lux_listing__window_start__year',
            --    'lux_listing__window_end',
            --    'lux_listing__window_end__week',
            --    'lux_listing__window_end__month',
            --    'lux_listing__window_end__quarter',
            --    'lux_listing__window_end__year',
            --    'lux_listing']
            SELECT
              subq_4.window_start
              , subq_4.window_start__week
              , subq_4.window_start__month
              , subq_4.window_start__quarter
              , subq_4.window_start__year
              , subq_4.window_end
              , subq_4.window_end__week
              , subq_4.window_end__month
              , subq_4.window_end__quarter
              , subq_4.window_end__year
              , subq_4.lux_listing__window_start
              , subq_4.lux_listing__window_start__week
              , subq_4.lux_listing__window_start__month
              , subq_4.lux_listing__window_start__quarter
              , subq_4.lux_listing__window_start__year
              , subq_4.lux_listing__window_end
              , subq_4.lux_listing__window_end__week
              , subq_4.lux_listing__window_end__month
              , subq_4.lux_listing__window_end__quarter
              , subq_4.lux_listing__window_end__year
              , subq_4.lux_listing
              , subq_4.is_confirmed_lux
              , subq_4.lux_listing__is_confirmed_lux
            FROM (
              -- Read Elements From Semantic Model 'lux_listings'
              SELECT
                lux_listings_src_10019.valid_from AS window_start
                , DATE_TRUNC(lux_listings_src_10019.valid_from, isoweek) AS window_start__week
                , DATE_TRUNC(lux_listings_src_10019.valid_from, month) AS window_start__month
                , DATE_TRUNC(lux_listings_src_10019.valid_from, quarter) AS window_start__quarter
                , DATE_TRUNC(lux_listings_src_10019.valid_from, isoyear) AS window_start__year
                , lux_listings_src_10019.valid_to AS window_end
                , DATE_TRUNC(lux_listings_src_10019.valid_to, isoweek) AS window_end__week
                , DATE_TRUNC(lux_listings_src_10019.valid_to, month) AS window_end__month
                , DATE_TRUNC(lux_listings_src_10019.valid_to, quarter) AS window_end__quarter
                , DATE_TRUNC(lux_listings_src_10019.valid_to, isoyear) AS window_end__year
                , lux_listings_src_10019.is_confirmed_lux
                , lux_listings_src_10019.valid_from AS lux_listing__window_start
                , DATE_TRUNC(lux_listings_src_10019.valid_from, isoweek) AS lux_listing__window_start__week
                , DATE_TRUNC(lux_listings_src_10019.valid_from, month) AS lux_listing__window_start__month
                , DATE_TRUNC(lux_listings_src_10019.valid_from, quarter) AS lux_listing__window_start__quarter
                , DATE_TRUNC(lux_listings_src_10019.valid_from, isoyear) AS lux_listing__window_start__year
                , lux_listings_src_10019.valid_to AS lux_listing__window_end
                , DATE_TRUNC(lux_listings_src_10019.valid_to, isoweek) AS lux_listing__window_end__week
                , DATE_TRUNC(lux_listings_src_10019.valid_to, month) AS lux_listing__window_end__month
                , DATE_TRUNC(lux_listings_src_10019.valid_to, quarter) AS lux_listing__window_end__quarter
                , DATE_TRUNC(lux_listings_src_10019.valid_to, isoyear) AS lux_listing__window_end__year
                , lux_listings_src_10019.is_confirmed_lux AS lux_listing__is_confirmed_lux
                , lux_listings_src_10019.lux_listing_id AS lux_listing
              FROM ***************************.dim_lux_listings lux_listings_src_10019
            ) subq_4
          ) subq_5
          ON
            subq_3.lux_listing = subq_5.lux_listing
        ) subq_6
      ) subq_7
      ON
        (
          subq_2.listing = subq_7.listing
        ) AND (
          (
            subq_2.metric_time >= subq_7.lux_listing__window_start
          ) AND (
            (
              subq_2.metric_time < subq_7.lux_listing__window_end
            ) OR (
              subq_7.lux_listing__window_end IS NULL
            )
          )
        )
    ) subq_8
  ) subq_9
  GROUP BY
    metric_time
    , listing__lux_listing__is_confirmed_lux
) subq_10
