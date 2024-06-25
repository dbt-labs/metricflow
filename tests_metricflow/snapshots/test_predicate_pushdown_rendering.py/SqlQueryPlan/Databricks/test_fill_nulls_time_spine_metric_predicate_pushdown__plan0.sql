-- Compute Metrics via Expressions
SELECT
  subq_33.metric_time__day
  , subq_33.listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_14.metric_time__day, subq_32.metric_time__day) AS metric_time__day
    , COALESCE(subq_14.listing__country_latest, subq_32.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_14.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_32.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_13.metric_time__day
      , subq_13.listing__country_latest
      , COALESCE(subq_13.bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_11.metric_time__day AS metric_time__day
        , subq_10.listing__country_latest AS listing__country_latest
        , subq_10.bookings AS bookings
      FROM (
        -- Time Spine
        SELECT
          subq_12.ds AS metric_time__day
        FROM ***************************.mf_time_spine subq_12
      ) subq_11
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          subq_9.metric_time__day
          , subq_9.listing__country_latest
          , SUM(subq_9.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
          SELECT
            subq_8.metric_time__day
            , subq_8.listing__country_latest
            , subq_8.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_7.metric_time__day
              , subq_7.booking__is_instant
              , subq_7.listing__country_latest
              , subq_7.bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
              SELECT
                subq_6.metric_time__day
                , subq_6.booking__is_instant
                , subq_6.listing__country_latest
                , subq_6.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_2.metric_time__day AS metric_time__day
                  , subq_2.listing AS listing
                  , subq_2.booking__is_instant AS booking__is_instant
                  , subq_5.country_latest AS listing__country_latest
                  , subq_2.bookings AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
                  SELECT
                    subq_1.metric_time__day
                    , subq_1.listing
                    , subq_1.booking__is_instant
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
                        , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
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
                  -- Pass Only Elements: ['country_latest', 'listing']
                  SELECT
                    subq_4.listing
                    , subq_4.country_latest
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
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
            WHERE booking__is_instant
          ) subq_8
        ) subq_9
        GROUP BY
          subq_9.metric_time__day
          , subq_9.listing__country_latest
      ) subq_10
      ON
        subq_11.metric_time__day = subq_10.metric_time__day
    ) subq_13
  ) subq_14
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_31.metric_time__day
      , subq_31.listing__country_latest
      , COALESCE(subq_31.bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_29.metric_time__day AS metric_time__day
        , subq_28.listing__country_latest AS listing__country_latest
        , subq_28.bookings AS bookings
      FROM (
        -- Time Spine
        SELECT
          subq_30.ds AS metric_time__day
        FROM ***************************.mf_time_spine subq_30
      ) subq_29
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          subq_27.metric_time__day
          , subq_27.listing__country_latest
          , SUM(subq_27.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
          SELECT
            subq_26.metric_time__day
            , subq_26.listing__country_latest
            , subq_26.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_25.metric_time__day
              , subq_25.booking__is_instant
              , subq_25.listing__country_latest
              , subq_25.bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
              SELECT
                subq_24.metric_time__day
                , subq_24.booking__is_instant
                , subq_24.listing__country_latest
                , subq_24.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_20.metric_time__day AS metric_time__day
                  , subq_20.listing AS listing
                  , subq_20.booking__is_instant AS booking__is_instant
                  , subq_23.country_latest AS listing__country_latest
                  , subq_20.bookings AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
                  SELECT
                    subq_19.metric_time__day
                    , subq_19.listing
                    , subq_19.booking__is_instant
                    , subq_19.bookings
                  FROM (
                    -- Join to Time Spine Dataset
                    SELECT
                      subq_17.metric_time__day AS metric_time__day
                      , DATE_TRUNC('week', subq_17.metric_time__day) AS metric_time__week
                      , DATE_TRUNC('month', subq_17.metric_time__day) AS metric_time__month
                      , DATE_TRUNC('quarter', subq_17.metric_time__day) AS metric_time__quarter
                      , DATE_TRUNC('year', subq_17.metric_time__day) AS metric_time__year
                      , EXTRACT(year FROM subq_17.metric_time__day) AS metric_time__extract_year
                      , EXTRACT(quarter FROM subq_17.metric_time__day) AS metric_time__extract_quarter
                      , EXTRACT(month FROM subq_17.metric_time__day) AS metric_time__extract_month
                      , EXTRACT(day FROM subq_17.metric_time__day) AS metric_time__extract_day
                      , EXTRACT(DAYOFWEEK_ISO FROM subq_17.metric_time__day) AS metric_time__extract_dow
                      , EXTRACT(doy FROM subq_17.metric_time__day) AS metric_time__extract_doy
                      , subq_16.ds__day AS ds__day
                      , subq_16.ds__week AS ds__week
                      , subq_16.ds__month AS ds__month
                      , subq_16.ds__quarter AS ds__quarter
                      , subq_16.ds__year AS ds__year
                      , subq_16.ds__extract_year AS ds__extract_year
                      , subq_16.ds__extract_quarter AS ds__extract_quarter
                      , subq_16.ds__extract_month AS ds__extract_month
                      , subq_16.ds__extract_day AS ds__extract_day
                      , subq_16.ds__extract_dow AS ds__extract_dow
                      , subq_16.ds__extract_doy AS ds__extract_doy
                      , subq_16.ds_partitioned__day AS ds_partitioned__day
                      , subq_16.ds_partitioned__week AS ds_partitioned__week
                      , subq_16.ds_partitioned__month AS ds_partitioned__month
                      , subq_16.ds_partitioned__quarter AS ds_partitioned__quarter
                      , subq_16.ds_partitioned__year AS ds_partitioned__year
                      , subq_16.ds_partitioned__extract_year AS ds_partitioned__extract_year
                      , subq_16.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                      , subq_16.ds_partitioned__extract_month AS ds_partitioned__extract_month
                      , subq_16.ds_partitioned__extract_day AS ds_partitioned__extract_day
                      , subq_16.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                      , subq_16.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                      , subq_16.paid_at__day AS paid_at__day
                      , subq_16.paid_at__week AS paid_at__week
                      , subq_16.paid_at__month AS paid_at__month
                      , subq_16.paid_at__quarter AS paid_at__quarter
                      , subq_16.paid_at__year AS paid_at__year
                      , subq_16.paid_at__extract_year AS paid_at__extract_year
                      , subq_16.paid_at__extract_quarter AS paid_at__extract_quarter
                      , subq_16.paid_at__extract_month AS paid_at__extract_month
                      , subq_16.paid_at__extract_day AS paid_at__extract_day
                      , subq_16.paid_at__extract_dow AS paid_at__extract_dow
                      , subq_16.paid_at__extract_doy AS paid_at__extract_doy
                      , subq_16.booking__ds__day AS booking__ds__day
                      , subq_16.booking__ds__week AS booking__ds__week
                      , subq_16.booking__ds__month AS booking__ds__month
                      , subq_16.booking__ds__quarter AS booking__ds__quarter
                      , subq_16.booking__ds__year AS booking__ds__year
                      , subq_16.booking__ds__extract_year AS booking__ds__extract_year
                      , subq_16.booking__ds__extract_quarter AS booking__ds__extract_quarter
                      , subq_16.booking__ds__extract_month AS booking__ds__extract_month
                      , subq_16.booking__ds__extract_day AS booking__ds__extract_day
                      , subq_16.booking__ds__extract_dow AS booking__ds__extract_dow
                      , subq_16.booking__ds__extract_doy AS booking__ds__extract_doy
                      , subq_16.booking__ds_partitioned__day AS booking__ds_partitioned__day
                      , subq_16.booking__ds_partitioned__week AS booking__ds_partitioned__week
                      , subq_16.booking__ds_partitioned__month AS booking__ds_partitioned__month
                      , subq_16.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                      , subq_16.booking__ds_partitioned__year AS booking__ds_partitioned__year
                      , subq_16.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                      , subq_16.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                      , subq_16.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                      , subq_16.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                      , subq_16.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                      , subq_16.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                      , subq_16.booking__paid_at__day AS booking__paid_at__day
                      , subq_16.booking__paid_at__week AS booking__paid_at__week
                      , subq_16.booking__paid_at__month AS booking__paid_at__month
                      , subq_16.booking__paid_at__quarter AS booking__paid_at__quarter
                      , subq_16.booking__paid_at__year AS booking__paid_at__year
                      , subq_16.booking__paid_at__extract_year AS booking__paid_at__extract_year
                      , subq_16.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                      , subq_16.booking__paid_at__extract_month AS booking__paid_at__extract_month
                      , subq_16.booking__paid_at__extract_day AS booking__paid_at__extract_day
                      , subq_16.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                      , subq_16.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                      , subq_16.listing AS listing
                      , subq_16.guest AS guest
                      , subq_16.host AS host
                      , subq_16.booking__listing AS booking__listing
                      , subq_16.booking__guest AS booking__guest
                      , subq_16.booking__host AS booking__host
                      , subq_16.is_instant AS is_instant
                      , subq_16.booking__is_instant AS booking__is_instant
                      , subq_16.bookings AS bookings
                      , subq_16.instant_bookings AS instant_bookings
                      , subq_16.booking_value AS booking_value
                      , subq_16.max_booking_value AS max_booking_value
                      , subq_16.min_booking_value AS min_booking_value
                      , subq_16.bookers AS bookers
                      , subq_16.average_booking_value AS average_booking_value
                      , subq_16.referred_bookings AS referred_bookings
                      , subq_16.median_booking_value AS median_booking_value
                      , subq_16.booking_value_p99 AS booking_value_p99
                      , subq_16.discrete_booking_value_p99 AS discrete_booking_value_p99
                      , subq_16.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                      , subq_16.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                    FROM (
                      -- Time Spine
                      SELECT
                        subq_18.ds AS metric_time__day
                      FROM ***************************.mf_time_spine subq_18
                    ) subq_17
                    INNER JOIN (
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
                        , subq_15.ds_partitioned__day
                        , subq_15.ds_partitioned__week
                        , subq_15.ds_partitioned__month
                        , subq_15.ds_partitioned__quarter
                        , subq_15.ds_partitioned__year
                        , subq_15.ds_partitioned__extract_year
                        , subq_15.ds_partitioned__extract_quarter
                        , subq_15.ds_partitioned__extract_month
                        , subq_15.ds_partitioned__extract_day
                        , subq_15.ds_partitioned__extract_dow
                        , subq_15.ds_partitioned__extract_doy
                        , subq_15.paid_at__day
                        , subq_15.paid_at__week
                        , subq_15.paid_at__month
                        , subq_15.paid_at__quarter
                        , subq_15.paid_at__year
                        , subq_15.paid_at__extract_year
                        , subq_15.paid_at__extract_quarter
                        , subq_15.paid_at__extract_month
                        , subq_15.paid_at__extract_day
                        , subq_15.paid_at__extract_dow
                        , subq_15.paid_at__extract_doy
                        , subq_15.booking__ds__day
                        , subq_15.booking__ds__week
                        , subq_15.booking__ds__month
                        , subq_15.booking__ds__quarter
                        , subq_15.booking__ds__year
                        , subq_15.booking__ds__extract_year
                        , subq_15.booking__ds__extract_quarter
                        , subq_15.booking__ds__extract_month
                        , subq_15.booking__ds__extract_day
                        , subq_15.booking__ds__extract_dow
                        , subq_15.booking__ds__extract_doy
                        , subq_15.booking__ds_partitioned__day
                        , subq_15.booking__ds_partitioned__week
                        , subq_15.booking__ds_partitioned__month
                        , subq_15.booking__ds_partitioned__quarter
                        , subq_15.booking__ds_partitioned__year
                        , subq_15.booking__ds_partitioned__extract_year
                        , subq_15.booking__ds_partitioned__extract_quarter
                        , subq_15.booking__ds_partitioned__extract_month
                        , subq_15.booking__ds_partitioned__extract_day
                        , subq_15.booking__ds_partitioned__extract_dow
                        , subq_15.booking__ds_partitioned__extract_doy
                        , subq_15.booking__paid_at__day
                        , subq_15.booking__paid_at__week
                        , subq_15.booking__paid_at__month
                        , subq_15.booking__paid_at__quarter
                        , subq_15.booking__paid_at__year
                        , subq_15.booking__paid_at__extract_year
                        , subq_15.booking__paid_at__extract_quarter
                        , subq_15.booking__paid_at__extract_month
                        , subq_15.booking__paid_at__extract_day
                        , subq_15.booking__paid_at__extract_dow
                        , subq_15.booking__paid_at__extract_doy
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
                        , subq_15.guest
                        , subq_15.host
                        , subq_15.booking__listing
                        , subq_15.booking__guest
                        , subq_15.booking__host
                        , subq_15.is_instant
                        , subq_15.booking__is_instant
                        , subq_15.bookings
                        , subq_15.instant_bookings
                        , subq_15.booking_value
                        , subq_15.max_booking_value
                        , subq_15.min_booking_value
                        , subq_15.bookers
                        , subq_15.average_booking_value
                        , subq_15.referred_bookings
                        , subq_15.median_booking_value
                        , subq_15.booking_value_p99
                        , subq_15.discrete_booking_value_p99
                        , subq_15.approximate_continuous_booking_value_p99
                        , subq_15.approximate_discrete_booking_value_p99
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
                          , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                          , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                          , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                          , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                          , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                          , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                          , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                          , bookings_source_src_28000.listing_id AS listing
                          , bookings_source_src_28000.guest_id AS guest
                          , bookings_source_src_28000.host_id AS host
                          , bookings_source_src_28000.listing_id AS booking__listing
                          , bookings_source_src_28000.guest_id AS booking__guest
                          , bookings_source_src_28000.host_id AS booking__host
                        FROM ***************************.fct_bookings bookings_source_src_28000
                      ) subq_15
                    ) subq_16
                    ON
                      DATEADD(day, -14, subq_17.metric_time__day) = subq_16.metric_time__day
                  ) subq_19
                ) subq_20
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['country_latest', 'listing']
                  SELECT
                    subq_22.listing
                    , subq_22.country_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_21.ds__day
                      , subq_21.ds__week
                      , subq_21.ds__month
                      , subq_21.ds__quarter
                      , subq_21.ds__year
                      , subq_21.ds__extract_year
                      , subq_21.ds__extract_quarter
                      , subq_21.ds__extract_month
                      , subq_21.ds__extract_day
                      , subq_21.ds__extract_dow
                      , subq_21.ds__extract_doy
                      , subq_21.created_at__day
                      , subq_21.created_at__week
                      , subq_21.created_at__month
                      , subq_21.created_at__quarter
                      , subq_21.created_at__year
                      , subq_21.created_at__extract_year
                      , subq_21.created_at__extract_quarter
                      , subq_21.created_at__extract_month
                      , subq_21.created_at__extract_day
                      , subq_21.created_at__extract_dow
                      , subq_21.created_at__extract_doy
                      , subq_21.listing__ds__day
                      , subq_21.listing__ds__week
                      , subq_21.listing__ds__month
                      , subq_21.listing__ds__quarter
                      , subq_21.listing__ds__year
                      , subq_21.listing__ds__extract_year
                      , subq_21.listing__ds__extract_quarter
                      , subq_21.listing__ds__extract_month
                      , subq_21.listing__ds__extract_day
                      , subq_21.listing__ds__extract_dow
                      , subq_21.listing__ds__extract_doy
                      , subq_21.listing__created_at__day
                      , subq_21.listing__created_at__week
                      , subq_21.listing__created_at__month
                      , subq_21.listing__created_at__quarter
                      , subq_21.listing__created_at__year
                      , subq_21.listing__created_at__extract_year
                      , subq_21.listing__created_at__extract_quarter
                      , subq_21.listing__created_at__extract_month
                      , subq_21.listing__created_at__extract_day
                      , subq_21.listing__created_at__extract_dow
                      , subq_21.listing__created_at__extract_doy
                      , subq_21.ds__day AS metric_time__day
                      , subq_21.ds__week AS metric_time__week
                      , subq_21.ds__month AS metric_time__month
                      , subq_21.ds__quarter AS metric_time__quarter
                      , subq_21.ds__year AS metric_time__year
                      , subq_21.ds__extract_year AS metric_time__extract_year
                      , subq_21.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_21.ds__extract_month AS metric_time__extract_month
                      , subq_21.ds__extract_day AS metric_time__extract_day
                      , subq_21.ds__extract_dow AS metric_time__extract_dow
                      , subq_21.ds__extract_doy AS metric_time__extract_doy
                      , subq_21.listing
                      , subq_21.user
                      , subq_21.listing__user
                      , subq_21.country_latest
                      , subq_21.is_lux_latest
                      , subq_21.capacity_latest
                      , subq_21.listing__country_latest
                      , subq_21.listing__is_lux_latest
                      , subq_21.listing__capacity_latest
                      , subq_21.listings
                      , subq_21.largest_listing
                      , subq_21.smallest_listing
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                        , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                        , listings_latest_src_28000.country AS listing__country_latest
                        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                        , listings_latest_src_28000.capacity AS listing__capacity_latest
                        , listings_latest_src_28000.listing_id AS listing
                        , listings_latest_src_28000.user_id AS user
                        , listings_latest_src_28000.user_id AS listing__user
                      FROM ***************************.dim_listings_latest listings_latest_src_28000
                    ) subq_21
                  ) subq_22
                ) subq_23
                ON
                  subq_20.listing = subq_23.listing
              ) subq_24
            ) subq_25
            WHERE booking__is_instant
          ) subq_26
        ) subq_27
        GROUP BY
          subq_27.metric_time__day
          , subq_27.listing__country_latest
      ) subq_28
      ON
        subq_29.metric_time__day = subq_28.metric_time__day
    ) subq_31
  ) subq_32
  ON
    (
      subq_14.listing__country_latest = subq_32.listing__country_latest
    ) AND (
      subq_14.metric_time__day = subq_32.metric_time__day
    )
  GROUP BY
    COALESCE(subq_14.metric_time__day, subq_32.metric_time__day)
    , COALESCE(subq_14.listing__country_latest, subq_32.listing__country_latest)
) subq_33
