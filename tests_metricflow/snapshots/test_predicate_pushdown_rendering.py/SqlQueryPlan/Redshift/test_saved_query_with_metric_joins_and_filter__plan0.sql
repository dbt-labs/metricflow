-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_11.listing__capacity_latest, subq_23.listing__capacity_latest, subq_49.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_11.bookings) AS bookings
  , MAX(subq_23.views) AS views
  , MAX(subq_49.bookings_per_view) AS bookings_per_view
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_10.listing__capacity_latest
    , subq_10.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_9.listing__capacity_latest
      , SUM(subq_9.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
      SELECT
        subq_8.listing__capacity_latest
        , subq_8.bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_7.metric_time__day
          , subq_7.listing__is_lux_latest
          , subq_7.listing__capacity_latest
          , subq_7.bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
          SELECT
            subq_6.metric_time__day
            , subq_6.listing__is_lux_latest
            , subq_6.listing__capacity_latest
            , subq_6.bookings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_2.metric_time__day AS metric_time__day
              , subq_2.listing AS listing
              , subq_5.is_lux_latest AS listing__is_lux_latest
              , subq_5.capacity_latest AS listing__capacity_latest
              , subq_2.bookings AS bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
              SELECT
                subq_1.metric_time__day
                , subq_1.listing
                , subq_1.bookings
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_0.ds__day
                  , subq_0.ds__week
                  , subq_0.ds__month
                  , subq_0.ds__quarter
                  , subq_0.ds__year
                  , subq_0.ds__extract_year
                  , subq_0.ds__extract_quarter
                  , subq_0.ds__extract_month
                  , subq_0.ds__extract_day
                  , subq_0.ds__extract_dow
                  , subq_0.ds__extract_doy
                  , subq_0.ds_partitioned__day
                  , subq_0.ds_partitioned__week
                  , subq_0.ds_partitioned__month
                  , subq_0.ds_partitioned__quarter
                  , subq_0.ds_partitioned__year
                  , subq_0.ds_partitioned__extract_year
                  , subq_0.ds_partitioned__extract_quarter
                  , subq_0.ds_partitioned__extract_month
                  , subq_0.ds_partitioned__extract_day
                  , subq_0.ds_partitioned__extract_dow
                  , subq_0.ds_partitioned__extract_doy
                  , subq_0.paid_at__day
                  , subq_0.paid_at__week
                  , subq_0.paid_at__month
                  , subq_0.paid_at__quarter
                  , subq_0.paid_at__year
                  , subq_0.paid_at__extract_year
                  , subq_0.paid_at__extract_quarter
                  , subq_0.paid_at__extract_month
                  , subq_0.paid_at__extract_day
                  , subq_0.paid_at__extract_dow
                  , subq_0.paid_at__extract_doy
                  , subq_0.booking__ds__day
                  , subq_0.booking__ds__week
                  , subq_0.booking__ds__month
                  , subq_0.booking__ds__quarter
                  , subq_0.booking__ds__year
                  , subq_0.booking__ds__extract_year
                  , subq_0.booking__ds__extract_quarter
                  , subq_0.booking__ds__extract_month
                  , subq_0.booking__ds__extract_day
                  , subq_0.booking__ds__extract_dow
                  , subq_0.booking__ds__extract_doy
                  , subq_0.booking__ds_partitioned__day
                  , subq_0.booking__ds_partitioned__week
                  , subq_0.booking__ds_partitioned__month
                  , subq_0.booking__ds_partitioned__quarter
                  , subq_0.booking__ds_partitioned__year
                  , subq_0.booking__ds_partitioned__extract_year
                  , subq_0.booking__ds_partitioned__extract_quarter
                  , subq_0.booking__ds_partitioned__extract_month
                  , subq_0.booking__ds_partitioned__extract_day
                  , subq_0.booking__ds_partitioned__extract_dow
                  , subq_0.booking__ds_partitioned__extract_doy
                  , subq_0.booking__paid_at__day
                  , subq_0.booking__paid_at__week
                  , subq_0.booking__paid_at__month
                  , subq_0.booking__paid_at__quarter
                  , subq_0.booking__paid_at__year
                  , subq_0.booking__paid_at__extract_year
                  , subq_0.booking__paid_at__extract_quarter
                  , subq_0.booking__paid_at__extract_month
                  , subq_0.booking__paid_at__extract_day
                  , subq_0.booking__paid_at__extract_dow
                  , subq_0.booking__paid_at__extract_doy
                  , subq_0.ds__day AS metric_time__day
                  , subq_0.ds__week AS metric_time__week
                  , subq_0.ds__month AS metric_time__month
                  , subq_0.ds__quarter AS metric_time__quarter
                  , subq_0.ds__year AS metric_time__year
                  , subq_0.ds__extract_year AS metric_time__extract_year
                  , subq_0.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_0.ds__extract_month AS metric_time__extract_month
                  , subq_0.ds__extract_day AS metric_time__extract_day
                  , subq_0.ds__extract_dow AS metric_time__extract_dow
                  , subq_0.ds__extract_doy AS metric_time__extract_doy
                  , subq_0.listing
                  , subq_0.guest
                  , subq_0.host
                  , subq_0.booking__listing
                  , subq_0.booking__guest
                  , subq_0.booking__host
                  , subq_0.is_instant
                  , subq_0.booking__is_instant
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
                    , bookings_source_src_28000.booking_value
                    , bookings_source_src_28000.booking_value AS max_booking_value
                    , bookings_source_src_28000.booking_value AS min_booking_value
                    , bookings_source_src_28000.guest_id AS bookers
                    , bookings_source_src_28000.booking_value AS average_booking_value
                    , bookings_source_src_28000.booking_value AS booking_payments
                    , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                    , bookings_source_src_28000.booking_value AS median_booking_value
                    , bookings_source_src_28000.booking_value AS booking_value_p99
                    , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
                    , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
                    , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
                    , bookings_source_src_28000.is_instant
                    , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
                    , DATE_TRUNC('week', bookings_source_src_28000.ds) AS ds__week
                    , DATE_TRUNC('month', bookings_source_src_28000.ds) AS ds__month
                    , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS ds__quarter
                    , DATE_TRUNC('year', bookings_source_src_28000.ds) AS ds__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS ds__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.ds) AS ds__extract_doy
                    , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
                    , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
                    , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
                    , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                    , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                    , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                    , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS paid_at__day
                    , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS paid_at__week
                    , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS paid_at__month
                    , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
                    , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS paid_at__year
                    , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                    , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS paid_at__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                    , bookings_source_src_28000.is_instant AS booking__is_instant
                    , DATE_TRUNC('day', bookings_source_src_28000.ds) AS booking__ds__day
                    , DATE_TRUNC('week', bookings_source_src_28000.ds) AS booking__ds__week
                    , DATE_TRUNC('month', bookings_source_src_28000.ds) AS booking__ds__month
                    , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
                    , DATE_TRUNC('year', bookings_source_src_28000.ds) AS booking__ds__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS booking__ds__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                    , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
                    , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
                    , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
                    , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
                    , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                    , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS booking__ds_partitioned__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                    , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
                    , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
                    , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
                    , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
                    , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
                    , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                    , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS booking__paid_at__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                    , bookings_source_src_28000.listing_id AS listing
                    , bookings_source_src_28000.guest_id AS guest
                    , bookings_source_src_28000.host_id AS host
                    , bookings_source_src_28000.listing_id AS booking__listing
                    , bookings_source_src_28000.guest_id AS booking__guest
                    , bookings_source_src_28000.host_id AS booking__host
                  FROM ***************************.fct_bookings bookings_source_src_28000
                ) subq_0
              ) subq_1
            ) subq_2
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
              SELECT
                subq_4.listing
                , subq_4.is_lux_latest
                , subq_4.capacity_latest
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_3.ds__day
                  , subq_3.ds__week
                  , subq_3.ds__month
                  , subq_3.ds__quarter
                  , subq_3.ds__year
                  , subq_3.ds__extract_year
                  , subq_3.ds__extract_quarter
                  , subq_3.ds__extract_month
                  , subq_3.ds__extract_day
                  , subq_3.ds__extract_dow
                  , subq_3.ds__extract_doy
                  , subq_3.created_at__day
                  , subq_3.created_at__week
                  , subq_3.created_at__month
                  , subq_3.created_at__quarter
                  , subq_3.created_at__year
                  , subq_3.created_at__extract_year
                  , subq_3.created_at__extract_quarter
                  , subq_3.created_at__extract_month
                  , subq_3.created_at__extract_day
                  , subq_3.created_at__extract_dow
                  , subq_3.created_at__extract_doy
                  , subq_3.listing__ds__day
                  , subq_3.listing__ds__week
                  , subq_3.listing__ds__month
                  , subq_3.listing__ds__quarter
                  , subq_3.listing__ds__year
                  , subq_3.listing__ds__extract_year
                  , subq_3.listing__ds__extract_quarter
                  , subq_3.listing__ds__extract_month
                  , subq_3.listing__ds__extract_day
                  , subq_3.listing__ds__extract_dow
                  , subq_3.listing__ds__extract_doy
                  , subq_3.listing__created_at__day
                  , subq_3.listing__created_at__week
                  , subq_3.listing__created_at__month
                  , subq_3.listing__created_at__quarter
                  , subq_3.listing__created_at__year
                  , subq_3.listing__created_at__extract_year
                  , subq_3.listing__created_at__extract_quarter
                  , subq_3.listing__created_at__extract_month
                  , subq_3.listing__created_at__extract_day
                  , subq_3.listing__created_at__extract_dow
                  , subq_3.listing__created_at__extract_doy
                  , subq_3.ds__day AS metric_time__day
                  , subq_3.ds__week AS metric_time__week
                  , subq_3.ds__month AS metric_time__month
                  , subq_3.ds__quarter AS metric_time__quarter
                  , subq_3.ds__year AS metric_time__year
                  , subq_3.ds__extract_year AS metric_time__extract_year
                  , subq_3.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_3.ds__extract_month AS metric_time__extract_month
                  , subq_3.ds__extract_day AS metric_time__extract_day
                  , subq_3.ds__extract_dow AS metric_time__extract_dow
                  , subq_3.ds__extract_doy AS metric_time__extract_doy
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
                    , listings_latest_src_28000.capacity AS largest_listing
                    , listings_latest_src_28000.capacity AS smallest_listing
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                    , listings_latest_src_28000.country AS country_latest
                    , listings_latest_src_28000.is_lux AS is_lux_latest
                    , listings_latest_src_28000.capacity AS capacity_latest
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                    , listings_latest_src_28000.country AS listing__country_latest
                    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                    , listings_latest_src_28000.capacity AS listing__capacity_latest
                    , listings_latest_src_28000.listing_id AS listing
                    , listings_latest_src_28000.user_id AS user
                    , listings_latest_src_28000.user_id AS listing__user
                  FROM ***************************.dim_listings_latest listings_latest_src_28000
                ) subq_3
              ) subq_4
            ) subq_5
            ON
              subq_2.listing = subq_5.listing
          ) subq_6
        ) subq_7
        WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
      ) subq_8
    ) subq_9
    GROUP BY
      subq_9.listing__capacity_latest
  ) subq_10
) subq_11
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_22.listing__capacity_latest
    , subq_22.views
  FROM (
    -- Aggregate Measures
    SELECT
      subq_21.listing__capacity_latest
      , SUM(subq_21.views) AS views
    FROM (
      -- Pass Only Elements: ['views', 'listing__capacity_latest']
      SELECT
        subq_20.listing__capacity_latest
        , subq_20.views
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_19.metric_time__day
          , subq_19.listing__is_lux_latest
          , subq_19.listing__capacity_latest
          , subq_19.views
        FROM (
          -- Pass Only Elements: ['views', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
          SELECT
            subq_18.metric_time__day
            , subq_18.listing__is_lux_latest
            , subq_18.listing__capacity_latest
            , subq_18.views
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_14.metric_time__day AS metric_time__day
              , subq_14.listing AS listing
              , subq_17.is_lux_latest AS listing__is_lux_latest
              , subq_17.capacity_latest AS listing__capacity_latest
              , subq_14.views AS views
            FROM (
              -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
              SELECT
                subq_13.metric_time__day
                , subq_13.listing
                , subq_13.views
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_12.ds__day
                  , subq_12.ds__week
                  , subq_12.ds__month
                  , subq_12.ds__quarter
                  , subq_12.ds__year
                  , subq_12.ds__extract_year
                  , subq_12.ds__extract_quarter
                  , subq_12.ds__extract_month
                  , subq_12.ds__extract_day
                  , subq_12.ds__extract_dow
                  , subq_12.ds__extract_doy
                  , subq_12.ds_partitioned__day
                  , subq_12.ds_partitioned__week
                  , subq_12.ds_partitioned__month
                  , subq_12.ds_partitioned__quarter
                  , subq_12.ds_partitioned__year
                  , subq_12.ds_partitioned__extract_year
                  , subq_12.ds_partitioned__extract_quarter
                  , subq_12.ds_partitioned__extract_month
                  , subq_12.ds_partitioned__extract_day
                  , subq_12.ds_partitioned__extract_dow
                  , subq_12.ds_partitioned__extract_doy
                  , subq_12.view__ds__day
                  , subq_12.view__ds__week
                  , subq_12.view__ds__month
                  , subq_12.view__ds__quarter
                  , subq_12.view__ds__year
                  , subq_12.view__ds__extract_year
                  , subq_12.view__ds__extract_quarter
                  , subq_12.view__ds__extract_month
                  , subq_12.view__ds__extract_day
                  , subq_12.view__ds__extract_dow
                  , subq_12.view__ds__extract_doy
                  , subq_12.view__ds_partitioned__day
                  , subq_12.view__ds_partitioned__week
                  , subq_12.view__ds_partitioned__month
                  , subq_12.view__ds_partitioned__quarter
                  , subq_12.view__ds_partitioned__year
                  , subq_12.view__ds_partitioned__extract_year
                  , subq_12.view__ds_partitioned__extract_quarter
                  , subq_12.view__ds_partitioned__extract_month
                  , subq_12.view__ds_partitioned__extract_day
                  , subq_12.view__ds_partitioned__extract_dow
                  , subq_12.view__ds_partitioned__extract_doy
                  , subq_12.ds__day AS metric_time__day
                  , subq_12.ds__week AS metric_time__week
                  , subq_12.ds__month AS metric_time__month
                  , subq_12.ds__quarter AS metric_time__quarter
                  , subq_12.ds__year AS metric_time__year
                  , subq_12.ds__extract_year AS metric_time__extract_year
                  , subq_12.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_12.ds__extract_month AS metric_time__extract_month
                  , subq_12.ds__extract_day AS metric_time__extract_day
                  , subq_12.ds__extract_dow AS metric_time__extract_dow
                  , subq_12.ds__extract_doy AS metric_time__extract_doy
                  , subq_12.listing
                  , subq_12.user
                  , subq_12.view__listing
                  , subq_12.view__user
                  , subq_12.views
                FROM (
                  -- Read Elements From Semantic Model 'views_source'
                  SELECT
                    1 AS views
                    , DATE_TRUNC('day', views_source_src_28000.ds) AS ds__day
                    , DATE_TRUNC('week', views_source_src_28000.ds) AS ds__week
                    , DATE_TRUNC('month', views_source_src_28000.ds) AS ds__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds) AS ds__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds) AS ds__year
                    , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds) END AS ds__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds) AS ds__extract_doy
                    , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS ds_partitioned__day
                    , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS ds_partitioned__week
                    , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS ds_partitioned__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS ds_partitioned__year
                    , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                    , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                    , DATE_TRUNC('day', views_source_src_28000.ds) AS view__ds__day
                    , DATE_TRUNC('week', views_source_src_28000.ds) AS view__ds__week
                    , DATE_TRUNC('month', views_source_src_28000.ds) AS view__ds__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds) AS view__ds__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds) AS view__ds__year
                    , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds) END AS view__ds__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds) AS view__ds__extract_doy
                    , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__day
                    , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__week
                    , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__year
                    , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                    , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds_partitioned) END AS view__ds_partitioned__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                    , views_source_src_28000.listing_id AS listing
                    , views_source_src_28000.user_id AS user
                    , views_source_src_28000.listing_id AS view__listing
                    , views_source_src_28000.user_id AS view__user
                  FROM ***************************.fct_views views_source_src_28000
                ) subq_12
              ) subq_13
            ) subq_14
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
              SELECT
                subq_16.listing
                , subq_16.is_lux_latest
                , subq_16.capacity_latest
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_15.ds__day
                  , subq_15.ds__week
                  , subq_15.ds__month
                  , subq_15.ds__quarter
                  , subq_15.ds__year
                  , subq_15.ds__extract_year
                  , subq_15.ds__extract_quarter
                  , subq_15.ds__extract_month
                  , subq_15.ds__extract_day
                  , subq_15.ds__extract_dow
                  , subq_15.ds__extract_doy
                  , subq_15.created_at__day
                  , subq_15.created_at__week
                  , subq_15.created_at__month
                  , subq_15.created_at__quarter
                  , subq_15.created_at__year
                  , subq_15.created_at__extract_year
                  , subq_15.created_at__extract_quarter
                  , subq_15.created_at__extract_month
                  , subq_15.created_at__extract_day
                  , subq_15.created_at__extract_dow
                  , subq_15.created_at__extract_doy
                  , subq_15.listing__ds__day
                  , subq_15.listing__ds__week
                  , subq_15.listing__ds__month
                  , subq_15.listing__ds__quarter
                  , subq_15.listing__ds__year
                  , subq_15.listing__ds__extract_year
                  , subq_15.listing__ds__extract_quarter
                  , subq_15.listing__ds__extract_month
                  , subq_15.listing__ds__extract_day
                  , subq_15.listing__ds__extract_dow
                  , subq_15.listing__ds__extract_doy
                  , subq_15.listing__created_at__day
                  , subq_15.listing__created_at__week
                  , subq_15.listing__created_at__month
                  , subq_15.listing__created_at__quarter
                  , subq_15.listing__created_at__year
                  , subq_15.listing__created_at__extract_year
                  , subq_15.listing__created_at__extract_quarter
                  , subq_15.listing__created_at__extract_month
                  , subq_15.listing__created_at__extract_day
                  , subq_15.listing__created_at__extract_dow
                  , subq_15.listing__created_at__extract_doy
                  , subq_15.ds__day AS metric_time__day
                  , subq_15.ds__week AS metric_time__week
                  , subq_15.ds__month AS metric_time__month
                  , subq_15.ds__quarter AS metric_time__quarter
                  , subq_15.ds__year AS metric_time__year
                  , subq_15.ds__extract_year AS metric_time__extract_year
                  , subq_15.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_15.ds__extract_month AS metric_time__extract_month
                  , subq_15.ds__extract_day AS metric_time__extract_day
                  , subq_15.ds__extract_dow AS metric_time__extract_dow
                  , subq_15.ds__extract_doy AS metric_time__extract_doy
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
                    , listings_latest_src_28000.capacity AS largest_listing
                    , listings_latest_src_28000.capacity AS smallest_listing
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                    , listings_latest_src_28000.country AS country_latest
                    , listings_latest_src_28000.is_lux AS is_lux_latest
                    , listings_latest_src_28000.capacity AS capacity_latest
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                    , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                    , listings_latest_src_28000.country AS listing__country_latest
                    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                    , listings_latest_src_28000.capacity AS listing__capacity_latest
                    , listings_latest_src_28000.listing_id AS listing
                    , listings_latest_src_28000.user_id AS user
                    , listings_latest_src_28000.user_id AS listing__user
                  FROM ***************************.dim_listings_latest listings_latest_src_28000
                ) subq_15
              ) subq_16
            ) subq_17
            ON
              subq_14.listing = subq_17.listing
          ) subq_18
        ) subq_19
        WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
      ) subq_20
    ) subq_21
    GROUP BY
      subq_21.listing__capacity_latest
  ) subq_22
) subq_23
ON
  subq_11.listing__capacity_latest = subq_23.listing__capacity_latest
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_48.listing__capacity_latest
    , CAST(subq_48.bookings AS DOUBLE PRECISION) / CAST(NULLIF(subq_48.views, 0) AS DOUBLE PRECISION) AS bookings_per_view
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_35.listing__capacity_latest, subq_47.listing__capacity_latest) AS listing__capacity_latest
      , MAX(subq_35.bookings) AS bookings
      , MAX(subq_47.views) AS views
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_34.listing__capacity_latest
        , subq_34.bookings
      FROM (
        -- Aggregate Measures
        SELECT
          subq_33.listing__capacity_latest
          , SUM(subq_33.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
          SELECT
            subq_32.listing__capacity_latest
            , subq_32.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_31.metric_time__day
              , subq_31.listing__is_lux_latest
              , subq_31.listing__capacity_latest
              , subq_31.bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
              SELECT
                subq_30.metric_time__day
                , subq_30.listing__is_lux_latest
                , subq_30.listing__capacity_latest
                , subq_30.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_26.metric_time__day AS metric_time__day
                  , subq_26.listing AS listing
                  , subq_29.is_lux_latest AS listing__is_lux_latest
                  , subq_29.capacity_latest AS listing__capacity_latest
                  , subq_26.bookings AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
                  SELECT
                    subq_25.metric_time__day
                    , subq_25.listing
                    , subq_25.bookings
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_24.ds__day
                      , subq_24.ds__week
                      , subq_24.ds__month
                      , subq_24.ds__quarter
                      , subq_24.ds__year
                      , subq_24.ds__extract_year
                      , subq_24.ds__extract_quarter
                      , subq_24.ds__extract_month
                      , subq_24.ds__extract_day
                      , subq_24.ds__extract_dow
                      , subq_24.ds__extract_doy
                      , subq_24.ds_partitioned__day
                      , subq_24.ds_partitioned__week
                      , subq_24.ds_partitioned__month
                      , subq_24.ds_partitioned__quarter
                      , subq_24.ds_partitioned__year
                      , subq_24.ds_partitioned__extract_year
                      , subq_24.ds_partitioned__extract_quarter
                      , subq_24.ds_partitioned__extract_month
                      , subq_24.ds_partitioned__extract_day
                      , subq_24.ds_partitioned__extract_dow
                      , subq_24.ds_partitioned__extract_doy
                      , subq_24.paid_at__day
                      , subq_24.paid_at__week
                      , subq_24.paid_at__month
                      , subq_24.paid_at__quarter
                      , subq_24.paid_at__year
                      , subq_24.paid_at__extract_year
                      , subq_24.paid_at__extract_quarter
                      , subq_24.paid_at__extract_month
                      , subq_24.paid_at__extract_day
                      , subq_24.paid_at__extract_dow
                      , subq_24.paid_at__extract_doy
                      , subq_24.booking__ds__day
                      , subq_24.booking__ds__week
                      , subq_24.booking__ds__month
                      , subq_24.booking__ds__quarter
                      , subq_24.booking__ds__year
                      , subq_24.booking__ds__extract_year
                      , subq_24.booking__ds__extract_quarter
                      , subq_24.booking__ds__extract_month
                      , subq_24.booking__ds__extract_day
                      , subq_24.booking__ds__extract_dow
                      , subq_24.booking__ds__extract_doy
                      , subq_24.booking__ds_partitioned__day
                      , subq_24.booking__ds_partitioned__week
                      , subq_24.booking__ds_partitioned__month
                      , subq_24.booking__ds_partitioned__quarter
                      , subq_24.booking__ds_partitioned__year
                      , subq_24.booking__ds_partitioned__extract_year
                      , subq_24.booking__ds_partitioned__extract_quarter
                      , subq_24.booking__ds_partitioned__extract_month
                      , subq_24.booking__ds_partitioned__extract_day
                      , subq_24.booking__ds_partitioned__extract_dow
                      , subq_24.booking__ds_partitioned__extract_doy
                      , subq_24.booking__paid_at__day
                      , subq_24.booking__paid_at__week
                      , subq_24.booking__paid_at__month
                      , subq_24.booking__paid_at__quarter
                      , subq_24.booking__paid_at__year
                      , subq_24.booking__paid_at__extract_year
                      , subq_24.booking__paid_at__extract_quarter
                      , subq_24.booking__paid_at__extract_month
                      , subq_24.booking__paid_at__extract_day
                      , subq_24.booking__paid_at__extract_dow
                      , subq_24.booking__paid_at__extract_doy
                      , subq_24.ds__day AS metric_time__day
                      , subq_24.ds__week AS metric_time__week
                      , subq_24.ds__month AS metric_time__month
                      , subq_24.ds__quarter AS metric_time__quarter
                      , subq_24.ds__year AS metric_time__year
                      , subq_24.ds__extract_year AS metric_time__extract_year
                      , subq_24.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_24.ds__extract_month AS metric_time__extract_month
                      , subq_24.ds__extract_day AS metric_time__extract_day
                      , subq_24.ds__extract_dow AS metric_time__extract_dow
                      , subq_24.ds__extract_doy AS metric_time__extract_doy
                      , subq_24.listing
                      , subq_24.guest
                      , subq_24.host
                      , subq_24.booking__listing
                      , subq_24.booking__guest
                      , subq_24.booking__host
                      , subq_24.is_instant
                      , subq_24.booking__is_instant
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
                        , bookings_source_src_28000.booking_value
                        , bookings_source_src_28000.booking_value AS max_booking_value
                        , bookings_source_src_28000.booking_value AS min_booking_value
                        , bookings_source_src_28000.guest_id AS bookers
                        , bookings_source_src_28000.booking_value AS average_booking_value
                        , bookings_source_src_28000.booking_value AS booking_payments
                        , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                        , bookings_source_src_28000.booking_value AS median_booking_value
                        , bookings_source_src_28000.booking_value AS booking_value_p99
                        , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
                        , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
                        , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
                        , bookings_source_src_28000.is_instant
                        , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
                        , DATE_TRUNC('week', bookings_source_src_28000.ds) AS ds__week
                        , DATE_TRUNC('month', bookings_source_src_28000.ds) AS ds__month
                        , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS ds__quarter
                        , DATE_TRUNC('year', bookings_source_src_28000.ds) AS ds__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS ds__extract_dow
                        , EXTRACT(doy FROM bookings_source_src_28000.ds) AS ds__extract_doy
                        , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
                        , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
                        , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
                        , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                        , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                        , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
                        , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                        , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS paid_at__day
                        , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS paid_at__week
                        , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS paid_at__month
                        , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
                        , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS paid_at__year
                        , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                        , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS paid_at__extract_dow
                        , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                        , bookings_source_src_28000.is_instant AS booking__is_instant
                        , DATE_TRUNC('day', bookings_source_src_28000.ds) AS booking__ds__day
                        , DATE_TRUNC('week', bookings_source_src_28000.ds) AS booking__ds__week
                        , DATE_TRUNC('month', bookings_source_src_28000.ds) AS booking__ds__month
                        , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
                        , DATE_TRUNC('year', bookings_source_src_28000.ds) AS booking__ds__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS booking__ds__extract_dow
                        , EXTRACT(doy FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                        , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
                        , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
                        , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
                        , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
                        , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                        , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS booking__ds_partitioned__extract_dow
                        , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                        , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
                        , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
                        , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
                        , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
                        , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
                        , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                        , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS booking__paid_at__extract_dow
                        , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                        , bookings_source_src_28000.listing_id AS listing
                        , bookings_source_src_28000.guest_id AS guest
                        , bookings_source_src_28000.host_id AS host
                        , bookings_source_src_28000.listing_id AS booking__listing
                        , bookings_source_src_28000.guest_id AS booking__guest
                        , bookings_source_src_28000.host_id AS booking__host
                      FROM ***************************.fct_bookings bookings_source_src_28000
                    ) subq_24
                  ) subq_25
                ) subq_26
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
                  SELECT
                    subq_28.listing
                    , subq_28.is_lux_latest
                    , subq_28.capacity_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_27.ds__day
                      , subq_27.ds__week
                      , subq_27.ds__month
                      , subq_27.ds__quarter
                      , subq_27.ds__year
                      , subq_27.ds__extract_year
                      , subq_27.ds__extract_quarter
                      , subq_27.ds__extract_month
                      , subq_27.ds__extract_day
                      , subq_27.ds__extract_dow
                      , subq_27.ds__extract_doy
                      , subq_27.created_at__day
                      , subq_27.created_at__week
                      , subq_27.created_at__month
                      , subq_27.created_at__quarter
                      , subq_27.created_at__year
                      , subq_27.created_at__extract_year
                      , subq_27.created_at__extract_quarter
                      , subq_27.created_at__extract_month
                      , subq_27.created_at__extract_day
                      , subq_27.created_at__extract_dow
                      , subq_27.created_at__extract_doy
                      , subq_27.listing__ds__day
                      , subq_27.listing__ds__week
                      , subq_27.listing__ds__month
                      , subq_27.listing__ds__quarter
                      , subq_27.listing__ds__year
                      , subq_27.listing__ds__extract_year
                      , subq_27.listing__ds__extract_quarter
                      , subq_27.listing__ds__extract_month
                      , subq_27.listing__ds__extract_day
                      , subq_27.listing__ds__extract_dow
                      , subq_27.listing__ds__extract_doy
                      , subq_27.listing__created_at__day
                      , subq_27.listing__created_at__week
                      , subq_27.listing__created_at__month
                      , subq_27.listing__created_at__quarter
                      , subq_27.listing__created_at__year
                      , subq_27.listing__created_at__extract_year
                      , subq_27.listing__created_at__extract_quarter
                      , subq_27.listing__created_at__extract_month
                      , subq_27.listing__created_at__extract_day
                      , subq_27.listing__created_at__extract_dow
                      , subq_27.listing__created_at__extract_doy
                      , subq_27.ds__day AS metric_time__day
                      , subq_27.ds__week AS metric_time__week
                      , subq_27.ds__month AS metric_time__month
                      , subq_27.ds__quarter AS metric_time__quarter
                      , subq_27.ds__year AS metric_time__year
                      , subq_27.ds__extract_year AS metric_time__extract_year
                      , subq_27.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_27.ds__extract_month AS metric_time__extract_month
                      , subq_27.ds__extract_day AS metric_time__extract_day
                      , subq_27.ds__extract_dow AS metric_time__extract_dow
                      , subq_27.ds__extract_doy AS metric_time__extract_doy
                      , subq_27.listing
                      , subq_27.user
                      , subq_27.listing__user
                      , subq_27.country_latest
                      , subq_27.is_lux_latest
                      , subq_27.capacity_latest
                      , subq_27.listing__country_latest
                      , subq_27.listing__is_lux_latest
                      , subq_27.listing__capacity_latest
                      , subq_27.listings
                      , subq_27.largest_listing
                      , subq_27.smallest_listing
                    FROM (
                      -- Read Elements From Semantic Model 'listings_latest'
                      SELECT
                        1 AS listings
                        , listings_latest_src_28000.capacity AS largest_listing
                        , listings_latest_src_28000.capacity AS smallest_listing
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                        , listings_latest_src_28000.country AS country_latest
                        , listings_latest_src_28000.is_lux AS is_lux_latest
                        , listings_latest_src_28000.capacity AS capacity_latest
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                        , listings_latest_src_28000.country AS listing__country_latest
                        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                        , listings_latest_src_28000.capacity AS listing__capacity_latest
                        , listings_latest_src_28000.listing_id AS listing
                        , listings_latest_src_28000.user_id AS user
                        , listings_latest_src_28000.user_id AS listing__user
                      FROM ***************************.dim_listings_latest listings_latest_src_28000
                    ) subq_27
                  ) subq_28
                ) subq_29
                ON
                  subq_26.listing = subq_29.listing
              ) subq_30
            ) subq_31
            WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
          ) subq_32
        ) subq_33
        GROUP BY
          subq_33.listing__capacity_latest
      ) subq_34
    ) subq_35
    FULL OUTER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_46.listing__capacity_latest
        , subq_46.views
      FROM (
        -- Aggregate Measures
        SELECT
          subq_45.listing__capacity_latest
          , SUM(subq_45.views) AS views
        FROM (
          -- Pass Only Elements: ['views', 'listing__capacity_latest']
          SELECT
            subq_44.listing__capacity_latest
            , subq_44.views
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_43.metric_time__day
              , subq_43.listing__is_lux_latest
              , subq_43.listing__capacity_latest
              , subq_43.views
            FROM (
              -- Pass Only Elements: ['views', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
              SELECT
                subq_42.metric_time__day
                , subq_42.listing__is_lux_latest
                , subq_42.listing__capacity_latest
                , subq_42.views
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_38.metric_time__day AS metric_time__day
                  , subq_38.listing AS listing
                  , subq_41.is_lux_latest AS listing__is_lux_latest
                  , subq_41.capacity_latest AS listing__capacity_latest
                  , subq_38.views AS views
                FROM (
                  -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
                  SELECT
                    subq_37.metric_time__day
                    , subq_37.listing
                    , subq_37.views
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_36.ds__day
                      , subq_36.ds__week
                      , subq_36.ds__month
                      , subq_36.ds__quarter
                      , subq_36.ds__year
                      , subq_36.ds__extract_year
                      , subq_36.ds__extract_quarter
                      , subq_36.ds__extract_month
                      , subq_36.ds__extract_day
                      , subq_36.ds__extract_dow
                      , subq_36.ds__extract_doy
                      , subq_36.ds_partitioned__day
                      , subq_36.ds_partitioned__week
                      , subq_36.ds_partitioned__month
                      , subq_36.ds_partitioned__quarter
                      , subq_36.ds_partitioned__year
                      , subq_36.ds_partitioned__extract_year
                      , subq_36.ds_partitioned__extract_quarter
                      , subq_36.ds_partitioned__extract_month
                      , subq_36.ds_partitioned__extract_day
                      , subq_36.ds_partitioned__extract_dow
                      , subq_36.ds_partitioned__extract_doy
                      , subq_36.view__ds__day
                      , subq_36.view__ds__week
                      , subq_36.view__ds__month
                      , subq_36.view__ds__quarter
                      , subq_36.view__ds__year
                      , subq_36.view__ds__extract_year
                      , subq_36.view__ds__extract_quarter
                      , subq_36.view__ds__extract_month
                      , subq_36.view__ds__extract_day
                      , subq_36.view__ds__extract_dow
                      , subq_36.view__ds__extract_doy
                      , subq_36.view__ds_partitioned__day
                      , subq_36.view__ds_partitioned__week
                      , subq_36.view__ds_partitioned__month
                      , subq_36.view__ds_partitioned__quarter
                      , subq_36.view__ds_partitioned__year
                      , subq_36.view__ds_partitioned__extract_year
                      , subq_36.view__ds_partitioned__extract_quarter
                      , subq_36.view__ds_partitioned__extract_month
                      , subq_36.view__ds_partitioned__extract_day
                      , subq_36.view__ds_partitioned__extract_dow
                      , subq_36.view__ds_partitioned__extract_doy
                      , subq_36.ds__day AS metric_time__day
                      , subq_36.ds__week AS metric_time__week
                      , subq_36.ds__month AS metric_time__month
                      , subq_36.ds__quarter AS metric_time__quarter
                      , subq_36.ds__year AS metric_time__year
                      , subq_36.ds__extract_year AS metric_time__extract_year
                      , subq_36.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_36.ds__extract_month AS metric_time__extract_month
                      , subq_36.ds__extract_day AS metric_time__extract_day
                      , subq_36.ds__extract_dow AS metric_time__extract_dow
                      , subq_36.ds__extract_doy AS metric_time__extract_doy
                      , subq_36.listing
                      , subq_36.user
                      , subq_36.view__listing
                      , subq_36.view__user
                      , subq_36.views
                    FROM (
                      -- Read Elements From Semantic Model 'views_source'
                      SELECT
                        1 AS views
                        , DATE_TRUNC('day', views_source_src_28000.ds) AS ds__day
                        , DATE_TRUNC('week', views_source_src_28000.ds) AS ds__week
                        , DATE_TRUNC('month', views_source_src_28000.ds) AS ds__month
                        , DATE_TRUNC('quarter', views_source_src_28000.ds) AS ds__quarter
                        , DATE_TRUNC('year', views_source_src_28000.ds) AS ds__year
                        , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds) END AS ds__extract_dow
                        , EXTRACT(doy FROM views_source_src_28000.ds) AS ds__extract_doy
                        , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS ds_partitioned__day
                        , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS ds_partitioned__week
                        , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS ds_partitioned__month
                        , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                        , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS ds_partitioned__year
                        , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                        , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
                        , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                        , DATE_TRUNC('day', views_source_src_28000.ds) AS view__ds__day
                        , DATE_TRUNC('week', views_source_src_28000.ds) AS view__ds__week
                        , DATE_TRUNC('month', views_source_src_28000.ds) AS view__ds__month
                        , DATE_TRUNC('quarter', views_source_src_28000.ds) AS view__ds__quarter
                        , DATE_TRUNC('year', views_source_src_28000.ds) AS view__ds__year
                        , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds) END AS view__ds__extract_dow
                        , EXTRACT(doy FROM views_source_src_28000.ds) AS view__ds__extract_doy
                        , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__day
                        , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__week
                        , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__month
                        , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__quarter
                        , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__year
                        , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                        , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds_partitioned) END AS view__ds_partitioned__extract_dow
                        , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                        , views_source_src_28000.listing_id AS listing
                        , views_source_src_28000.user_id AS user
                        , views_source_src_28000.listing_id AS view__listing
                        , views_source_src_28000.user_id AS view__user
                      FROM ***************************.fct_views views_source_src_28000
                    ) subq_36
                  ) subq_37
                ) subq_38
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
                  SELECT
                    subq_40.listing
                    , subq_40.is_lux_latest
                    , subq_40.capacity_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_39.ds__day
                      , subq_39.ds__week
                      , subq_39.ds__month
                      , subq_39.ds__quarter
                      , subq_39.ds__year
                      , subq_39.ds__extract_year
                      , subq_39.ds__extract_quarter
                      , subq_39.ds__extract_month
                      , subq_39.ds__extract_day
                      , subq_39.ds__extract_dow
                      , subq_39.ds__extract_doy
                      , subq_39.created_at__day
                      , subq_39.created_at__week
                      , subq_39.created_at__month
                      , subq_39.created_at__quarter
                      , subq_39.created_at__year
                      , subq_39.created_at__extract_year
                      , subq_39.created_at__extract_quarter
                      , subq_39.created_at__extract_month
                      , subq_39.created_at__extract_day
                      , subq_39.created_at__extract_dow
                      , subq_39.created_at__extract_doy
                      , subq_39.listing__ds__day
                      , subq_39.listing__ds__week
                      , subq_39.listing__ds__month
                      , subq_39.listing__ds__quarter
                      , subq_39.listing__ds__year
                      , subq_39.listing__ds__extract_year
                      , subq_39.listing__ds__extract_quarter
                      , subq_39.listing__ds__extract_month
                      , subq_39.listing__ds__extract_day
                      , subq_39.listing__ds__extract_dow
                      , subq_39.listing__ds__extract_doy
                      , subq_39.listing__created_at__day
                      , subq_39.listing__created_at__week
                      , subq_39.listing__created_at__month
                      , subq_39.listing__created_at__quarter
                      , subq_39.listing__created_at__year
                      , subq_39.listing__created_at__extract_year
                      , subq_39.listing__created_at__extract_quarter
                      , subq_39.listing__created_at__extract_month
                      , subq_39.listing__created_at__extract_day
                      , subq_39.listing__created_at__extract_dow
                      , subq_39.listing__created_at__extract_doy
                      , subq_39.ds__day AS metric_time__day
                      , subq_39.ds__week AS metric_time__week
                      , subq_39.ds__month AS metric_time__month
                      , subq_39.ds__quarter AS metric_time__quarter
                      , subq_39.ds__year AS metric_time__year
                      , subq_39.ds__extract_year AS metric_time__extract_year
                      , subq_39.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_39.ds__extract_month AS metric_time__extract_month
                      , subq_39.ds__extract_day AS metric_time__extract_day
                      , subq_39.ds__extract_dow AS metric_time__extract_dow
                      , subq_39.ds__extract_doy AS metric_time__extract_doy
                      , subq_39.listing
                      , subq_39.user
                      , subq_39.listing__user
                      , subq_39.country_latest
                      , subq_39.is_lux_latest
                      , subq_39.capacity_latest
                      , subq_39.listing__country_latest
                      , subq_39.listing__is_lux_latest
                      , subq_39.listing__capacity_latest
                      , subq_39.listings
                      , subq_39.largest_listing
                      , subq_39.smallest_listing
                    FROM (
                      -- Read Elements From Semantic Model 'listings_latest'
                      SELECT
                        1 AS listings
                        , listings_latest_src_28000.capacity AS largest_listing
                        , listings_latest_src_28000.capacity AS smallest_listing
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                        , listings_latest_src_28000.country AS country_latest
                        , listings_latest_src_28000.is_lux AS is_lux_latest
                        , listings_latest_src_28000.capacity AS capacity_latest
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                        , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                        , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                        , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                        , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                        , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                        , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                        , listings_latest_src_28000.country AS listing__country_latest
                        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                        , listings_latest_src_28000.capacity AS listing__capacity_latest
                        , listings_latest_src_28000.listing_id AS listing
                        , listings_latest_src_28000.user_id AS user
                        , listings_latest_src_28000.user_id AS listing__user
                      FROM ***************************.dim_listings_latest listings_latest_src_28000
                    ) subq_39
                  ) subq_40
                ) subq_41
                ON
                  subq_38.listing = subq_41.listing
              ) subq_42
            ) subq_43
            WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
          ) subq_44
        ) subq_45
        GROUP BY
          subq_45.listing__capacity_latest
      ) subq_46
    ) subq_47
    ON
      subq_35.listing__capacity_latest = subq_47.listing__capacity_latest
    GROUP BY
      COALESCE(subq_35.listing__capacity_latest, subq_47.listing__capacity_latest)
  ) subq_48
) subq_49
ON
  COALESCE(subq_11.listing__capacity_latest, subq_23.listing__capacity_latest) = subq_49.listing__capacity_latest
GROUP BY
  COALESCE(subq_11.listing__capacity_latest, subq_23.listing__capacity_latest, subq_49.listing__capacity_latest)
