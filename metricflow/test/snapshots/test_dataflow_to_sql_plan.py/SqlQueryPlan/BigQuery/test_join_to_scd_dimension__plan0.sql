-- Compute Metrics via Expressions
SELECT
  subq_9.metric_time
  , subq_9.bookings AS family_bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_8.metric_time
    , SUM(subq_8.bookings) AS bookings
  FROM (
    -- Pass Only Elements:
    --   ['bookings', 'metric_time']
    SELECT
      subq_7.metric_time
      , subq_7.bookings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_6.metric_time
        , subq_6.listing__capacity
        , subq_6.bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'listing__capacity', 'metric_time']
        SELECT
          subq_5.metric_time
          , subq_5.listing__capacity
          , subq_5.bookings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.metric_time AS metric_time
            , subq_4.window_start AS listing__window_start
            , subq_4.window_end AS listing__window_end
            , subq_2.listing AS listing
            , subq_4.capacity AS listing__capacity
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
                , subq_0.paid_at
                , subq_0.paid_at__week
                , subq_0.paid_at__month
                , subq_0.paid_at__quarter
                , subq_0.paid_at__year
                , subq_0.booking__ds
                , subq_0.booking__ds__week
                , subq_0.booking__ds__month
                , subq_0.booking__ds__quarter
                , subq_0.booking__ds__year
                , subq_0.booking__ds_partitioned
                , subq_0.booking__ds_partitioned__week
                , subq_0.booking__ds_partitioned__month
                , subq_0.booking__ds_partitioned__quarter
                , subq_0.booking__ds_partitioned__year
                , subq_0.booking__paid_at
                , subq_0.booking__paid_at__week
                , subq_0.booking__paid_at__month
                , subq_0.booking__paid_at__quarter
                , subq_0.booking__paid_at__year
                , subq_0.ds AS metric_time
                , subq_0.ds__week AS metric_time__week
                , subq_0.ds__month AS metric_time__month
                , subq_0.ds__quarter AS metric_time__quarter
                , subq_0.ds__year AS metric_time__year
                , subq_0.listing
                , subq_0.guest
                , subq_0.host
                , subq_0.user
                , subq_0.booking__listing
                , subq_0.booking__guest
                , subq_0.booking__host
                , subq_0.booking__user
                , subq_0.is_instant
                , subq_0.booking__is_instant
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
                  , bookings_source_src_10015.paid_at
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, isoweek) AS paid_at__week
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, month) AS paid_at__month
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, quarter) AS paid_at__quarter
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, isoyear) AS paid_at__year
                  , bookings_source_src_10015.is_instant AS booking__is_instant
                  , bookings_source_src_10015.ds AS booking__ds
                  , DATE_TRUNC(bookings_source_src_10015.ds, isoweek) AS booking__ds__week
                  , DATE_TRUNC(bookings_source_src_10015.ds, month) AS booking__ds__month
                  , DATE_TRUNC(bookings_source_src_10015.ds, quarter) AS booking__ds__quarter
                  , DATE_TRUNC(bookings_source_src_10015.ds, isoyear) AS booking__ds__year
                  , bookings_source_src_10015.ds_partitioned AS booking__ds_partitioned
                  , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, isoweek) AS booking__ds_partitioned__week
                  , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, month) AS booking__ds_partitioned__month
                  , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
                  , DATE_TRUNC(bookings_source_src_10015.ds_partitioned, isoyear) AS booking__ds_partitioned__year
                  , bookings_source_src_10015.paid_at AS booking__paid_at
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, isoweek) AS booking__paid_at__week
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, month) AS booking__paid_at__month
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, quarter) AS booking__paid_at__quarter
                  , DATE_TRUNC(bookings_source_src_10015.paid_at, isoyear) AS booking__paid_at__year
                  , bookings_source_src_10015.listing_id AS listing
                  , bookings_source_src_10015.guest_id AS guest
                  , bookings_source_src_10015.host_id AS host
                  , bookings_source_src_10015.guest_id AS user
                  , bookings_source_src_10015.listing_id AS booking__listing
                  , bookings_source_src_10015.guest_id AS booking__guest
                  , bookings_source_src_10015.host_id AS booking__host
                  , bookings_source_src_10015.guest_id AS booking__user
                FROM ***************************.fct_bookings bookings_source_src_10015
              ) subq_0
            ) subq_1
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['capacity', 'window_start', 'window_end', 'listing']
            SELECT
              subq_3.window_start
              , subq_3.window_end
              , subq_3.listing
              , subq_3.capacity
            FROM (
              -- Read Elements From Semantic Model 'listings'
              SELECT
                listings_src_10017.active_from AS window_start
                , DATE_TRUNC(listings_src_10017.active_from, isoweek) AS window_start__week
                , DATE_TRUNC(listings_src_10017.active_from, month) AS window_start__month
                , DATE_TRUNC(listings_src_10017.active_from, quarter) AS window_start__quarter
                , DATE_TRUNC(listings_src_10017.active_from, isoyear) AS window_start__year
                , listings_src_10017.active_to AS window_end
                , DATE_TRUNC(listings_src_10017.active_to, isoweek) AS window_end__week
                , DATE_TRUNC(listings_src_10017.active_to, month) AS window_end__month
                , DATE_TRUNC(listings_src_10017.active_to, quarter) AS window_end__quarter
                , DATE_TRUNC(listings_src_10017.active_to, isoyear) AS window_end__year
                , listings_src_10017.country
                , listings_src_10017.is_lux
                , listings_src_10017.capacity
                , listings_src_10017.active_from AS listing__window_start
                , DATE_TRUNC(listings_src_10017.active_from, isoweek) AS listing__window_start__week
                , DATE_TRUNC(listings_src_10017.active_from, month) AS listing__window_start__month
                , DATE_TRUNC(listings_src_10017.active_from, quarter) AS listing__window_start__quarter
                , DATE_TRUNC(listings_src_10017.active_from, isoyear) AS listing__window_start__year
                , listings_src_10017.active_to AS listing__window_end
                , DATE_TRUNC(listings_src_10017.active_to, isoweek) AS listing__window_end__week
                , DATE_TRUNC(listings_src_10017.active_to, month) AS listing__window_end__month
                , DATE_TRUNC(listings_src_10017.active_to, quarter) AS listing__window_end__quarter
                , DATE_TRUNC(listings_src_10017.active_to, isoyear) AS listing__window_end__year
                , listings_src_10017.country AS listing__country
                , listings_src_10017.is_lux AS listing__is_lux
                , listings_src_10017.capacity AS listing__capacity
                , listings_src_10017.listing_id AS listing
                , listings_src_10017.user_id AS user
                , listings_src_10017.user_id AS listing__user
              FROM ***************************.dim_listings listings_src_10017
            ) subq_3
          ) subq_4
          ON
            (
              subq_2.listing = subq_4.listing
            ) AND (
              (
                subq_2.metric_time >= subq_4.window_start
              ) AND (
                (
                  subq_2.metric_time < subq_4.window_end
                ) OR (
                  subq_4.window_end IS NULL
                )
              )
            )
        ) subq_5
      ) subq_6
      WHERE listing__capacity > 2
    ) subq_7
  ) subq_8
  GROUP BY
    metric_time
) subq_9
