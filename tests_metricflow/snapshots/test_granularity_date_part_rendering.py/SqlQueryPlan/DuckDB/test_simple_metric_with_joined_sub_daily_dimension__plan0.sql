-- Compute Metrics via Expressions
SELECT
  subq_8.listing__user__bio_added_ts__minute
  , subq_8.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_7.listing__user__bio_added_ts__minute
    , SUM(subq_7.bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__user__bio_added_ts__minute']
    SELECT
      subq_1.bookings AS bookings
    FROM (
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'ds_partitioned__day', 'listing']
      SELECT
        subq_0.ds_partitioned__day
        , subq_0.listing
        , subq_0.bookings
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
          , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
          , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
          , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
          , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
          , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
          , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
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
    LEFT OUTER JOIN (
      -- Join Standard Outputs
      -- Pass Only Elements: ['user__ds_partitioned__day', 'user__bio_added_ts__minute', 'listing']
      SELECT
        subq_3.listing AS listing
      FROM (
        -- Metric Time Dimension 'ds'
        SELECT
          subq_2.ds__day
          , subq_2.ds__week
          , subq_2.ds__month
          , subq_2.ds__quarter
          , subq_2.ds__year
          , subq_2.ds__extract_year
          , subq_2.ds__extract_quarter
          , subq_2.ds__extract_month
          , subq_2.ds__extract_day
          , subq_2.ds__extract_dow
          , subq_2.ds__extract_doy
          , subq_2.created_at__day
          , subq_2.created_at__week
          , subq_2.created_at__month
          , subq_2.created_at__quarter
          , subq_2.created_at__year
          , subq_2.created_at__extract_year
          , subq_2.created_at__extract_quarter
          , subq_2.created_at__extract_month
          , subq_2.created_at__extract_day
          , subq_2.created_at__extract_dow
          , subq_2.created_at__extract_doy
          , subq_2.listing__ds__day
          , subq_2.listing__ds__week
          , subq_2.listing__ds__month
          , subq_2.listing__ds__quarter
          , subq_2.listing__ds__year
          , subq_2.listing__ds__extract_year
          , subq_2.listing__ds__extract_quarter
          , subq_2.listing__ds__extract_month
          , subq_2.listing__ds__extract_day
          , subq_2.listing__ds__extract_dow
          , subq_2.listing__ds__extract_doy
          , subq_2.listing__created_at__day
          , subq_2.listing__created_at__week
          , subq_2.listing__created_at__month
          , subq_2.listing__created_at__quarter
          , subq_2.listing__created_at__year
          , subq_2.listing__created_at__extract_year
          , subq_2.listing__created_at__extract_quarter
          , subq_2.listing__created_at__extract_month
          , subq_2.listing__created_at__extract_day
          , subq_2.listing__created_at__extract_dow
          , subq_2.listing__created_at__extract_doy
          , subq_2.ds__day AS metric_time__day
          , subq_2.ds__week AS metric_time__week
          , subq_2.ds__month AS metric_time__month
          , subq_2.ds__quarter AS metric_time__quarter
          , subq_2.ds__year AS metric_time__year
          , subq_2.ds__extract_year AS metric_time__extract_year
          , subq_2.ds__extract_quarter AS metric_time__extract_quarter
          , subq_2.ds__extract_month AS metric_time__extract_month
          , subq_2.ds__extract_day AS metric_time__extract_day
          , subq_2.ds__extract_dow AS metric_time__extract_dow
          , subq_2.ds__extract_doy AS metric_time__extract_doy
          , subq_2.listing
          , subq_2.user
          , subq_2.listing__user
          , subq_2.country_latest
          , subq_2.is_lux_latest
          , subq_2.capacity_latest
          , subq_2.listing__country_latest
          , subq_2.listing__is_lux_latest
          , subq_2.listing__capacity_latest
          , subq_2.listings
          , subq_2.largest_listing
          , subq_2.smallest_listing
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
            , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
            , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
            , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
            , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
            , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
            , listings_latest_src_28000.country AS listing__country_latest
            , listings_latest_src_28000.is_lux AS listing__is_lux_latest
            , listings_latest_src_28000.capacity AS listing__capacity_latest
            , listings_latest_src_28000.listing_id AS listing
            , listings_latest_src_28000.user_id AS user
            , listings_latest_src_28000.user_id AS listing__user
          FROM ***************************.dim_listings_latest listings_latest_src_28000
        ) subq_2
      ) subq_3
      LEFT OUTER JOIN (
        -- Metric Time Dimension 'created_at'
        -- Pass Only Elements: [
        --   'home_state',
        --   'user__home_state',
        --   'ds__day',
        --   'ds__week',
        --   'ds__month',
        --   'ds__quarter',
        --   'ds__year',
        --   'ds__extract_year',
        --   'ds__extract_quarter',
        --   'ds__extract_month',
        --   'ds__extract_day',
        --   'ds__extract_dow',
        --   'ds__extract_doy',
        --   'created_at__day',
        --   'created_at__week',
        --   'created_at__month',
        --   'created_at__quarter',
        --   'created_at__year',
        --   'created_at__extract_year',
        --   'created_at__extract_quarter',
        --   'created_at__extract_month',
        --   'created_at__extract_day',
        --   'created_at__extract_dow',
        --   'created_at__extract_doy',
        --   'ds_partitioned__day',
        --   'ds_partitioned__week',
        --   'ds_partitioned__month',
        --   'ds_partitioned__quarter',
        --   'ds_partitioned__year',
        --   'ds_partitioned__extract_year',
        --   'ds_partitioned__extract_quarter',
        --   'ds_partitioned__extract_month',
        --   'ds_partitioned__extract_day',
        --   'ds_partitioned__extract_dow',
        --   'ds_partitioned__extract_doy',
        --   'last_profile_edit_ts__millisecond',
        --   'last_profile_edit_ts__second',
        --   'last_profile_edit_ts__minute',
        --   'last_profile_edit_ts__hour',
        --   'last_profile_edit_ts__day',
        --   'last_profile_edit_ts__week',
        --   'last_profile_edit_ts__month',
        --   'last_profile_edit_ts__quarter',
        --   'last_profile_edit_ts__year',
        --   'last_profile_edit_ts__extract_year',
        --   'last_profile_edit_ts__extract_quarter',
        --   'last_profile_edit_ts__extract_month',
        --   'last_profile_edit_ts__extract_day',
        --   'last_profile_edit_ts__extract_dow',
        --   'last_profile_edit_ts__extract_doy',
        --   'bio_added_ts__second',
        --   'bio_added_ts__minute',
        --   'bio_added_ts__hour',
        --   'bio_added_ts__day',
        --   'bio_added_ts__week',
        --   'bio_added_ts__month',
        --   'bio_added_ts__quarter',
        --   'bio_added_ts__year',
        --   'bio_added_ts__extract_year',
        --   'bio_added_ts__extract_quarter',
        --   'bio_added_ts__extract_month',
        --   'bio_added_ts__extract_day',
        --   'bio_added_ts__extract_dow',
        --   'bio_added_ts__extract_doy',
        --   'last_login_ts__minute',
        --   'last_login_ts__hour',
        --   'last_login_ts__day',
        --   'last_login_ts__week',
        --   'last_login_ts__month',
        --   'last_login_ts__quarter',
        --   'last_login_ts__year',
        --   'last_login_ts__extract_year',
        --   'last_login_ts__extract_quarter',
        --   'last_login_ts__extract_month',
        --   'last_login_ts__extract_day',
        --   'last_login_ts__extract_dow',
        --   'last_login_ts__extract_doy',
        --   'archived_at__hour',
        --   'archived_at__day',
        --   'archived_at__week',
        --   'archived_at__month',
        --   'archived_at__quarter',
        --   'archived_at__year',
        --   'archived_at__extract_year',
        --   'archived_at__extract_quarter',
        --   'archived_at__extract_month',
        --   'archived_at__extract_day',
        --   'archived_at__extract_dow',
        --   'archived_at__extract_doy',
        --   'user__ds__day',
        --   'user__ds__week',
        --   'user__ds__month',
        --   'user__ds__quarter',
        --   'user__ds__year',
        --   'user__ds__extract_year',
        --   'user__ds__extract_quarter',
        --   'user__ds__extract_month',
        --   'user__ds__extract_day',
        --   'user__ds__extract_dow',
        --   'user__ds__extract_doy',
        --   'user__created_at__day',
        --   'user__created_at__week',
        --   'user__created_at__month',
        --   'user__created_at__quarter',
        --   'user__created_at__year',
        --   'user__created_at__extract_year',
        --   'user__created_at__extract_quarter',
        --   'user__created_at__extract_month',
        --   'user__created_at__extract_day',
        --   'user__created_at__extract_dow',
        --   'user__created_at__extract_doy',
        --   'user__ds_partitioned__day',
        --   'user__ds_partitioned__week',
        --   'user__ds_partitioned__month',
        --   'user__ds_partitioned__quarter',
        --   'user__ds_partitioned__year',
        --   'user__ds_partitioned__extract_year',
        --   'user__ds_partitioned__extract_quarter',
        --   'user__ds_partitioned__extract_month',
        --   'user__ds_partitioned__extract_day',
        --   'user__ds_partitioned__extract_dow',
        --   'user__ds_partitioned__extract_doy',
        --   'user__last_profile_edit_ts__millisecond',
        --   'user__last_profile_edit_ts__second',
        --   'user__last_profile_edit_ts__minute',
        --   'user__last_profile_edit_ts__hour',
        --   'user__last_profile_edit_ts__day',
        --   'user__last_profile_edit_ts__week',
        --   'user__last_profile_edit_ts__month',
        --   'user__last_profile_edit_ts__quarter',
        --   'user__last_profile_edit_ts__year',
        --   'user__last_profile_edit_ts__extract_year',
        --   'user__last_profile_edit_ts__extract_quarter',
        --   'user__last_profile_edit_ts__extract_month',
        --   'user__last_profile_edit_ts__extract_day',
        --   'user__last_profile_edit_ts__extract_dow',
        --   'user__last_profile_edit_ts__extract_doy',
        --   'user__bio_added_ts__second',
        --   'user__bio_added_ts__minute',
        --   'user__bio_added_ts__hour',
        --   'user__bio_added_ts__day',
        --   'user__bio_added_ts__week',
        --   'user__bio_added_ts__month',
        --   'user__bio_added_ts__quarter',
        --   'user__bio_added_ts__year',
        --   'user__bio_added_ts__extract_year',
        --   'user__bio_added_ts__extract_quarter',
        --   'user__bio_added_ts__extract_month',
        --   'user__bio_added_ts__extract_day',
        --   'user__bio_added_ts__extract_dow',
        --   'user__bio_added_ts__extract_doy',
        --   'user__last_login_ts__minute',
        --   'user__last_login_ts__hour',
        --   'user__last_login_ts__day',
        --   'user__last_login_ts__week',
        --   'user__last_login_ts__month',
        --   'user__last_login_ts__quarter',
        --   'user__last_login_ts__year',
        --   'user__last_login_ts__extract_year',
        --   'user__last_login_ts__extract_quarter',
        --   'user__last_login_ts__extract_month',
        --   'user__last_login_ts__extract_day',
        --   'user__last_login_ts__extract_dow',
        --   'user__last_login_ts__extract_doy',
        --   'user__archived_at__hour',
        --   'user__archived_at__day',
        --   'user__archived_at__week',
        --   'user__archived_at__month',
        --   'user__archived_at__quarter',
        --   'user__archived_at__year',
        --   'user__archived_at__extract_year',
        --   'user__archived_at__extract_quarter',
        --   'user__archived_at__extract_month',
        --   'user__archived_at__extract_day',
        --   'user__archived_at__extract_dow',
        --   'user__archived_at__extract_doy',
        --   'metric_time__day',
        --   'metric_time__week',
        --   'metric_time__month',
        --   'metric_time__quarter',
        --   'metric_time__year',
        --   'metric_time__extract_year',
        --   'metric_time__extract_quarter',
        --   'metric_time__extract_month',
        --   'metric_time__extract_day',
        --   'metric_time__extract_dow',
        --   'metric_time__extract_doy',
        --   'user',
        -- ]
        SELECT
          subq_4.ds__day
          , subq_4.ds__week
          , subq_4.ds__month
          , subq_4.ds__quarter
          , subq_4.ds__year
          , subq_4.ds__extract_year
          , subq_4.ds__extract_quarter
          , subq_4.ds__extract_month
          , subq_4.ds__extract_day
          , subq_4.ds__extract_dow
          , subq_4.ds__extract_doy
          , subq_4.created_at__day
          , subq_4.created_at__week
          , subq_4.created_at__month
          , subq_4.created_at__quarter
          , subq_4.created_at__year
          , subq_4.created_at__extract_year
          , subq_4.created_at__extract_quarter
          , subq_4.created_at__extract_month
          , subq_4.created_at__extract_day
          , subq_4.created_at__extract_dow
          , subq_4.created_at__extract_doy
          , subq_4.ds_partitioned__day
          , subq_4.ds_partitioned__week
          , subq_4.ds_partitioned__month
          , subq_4.ds_partitioned__quarter
          , subq_4.ds_partitioned__year
          , subq_4.ds_partitioned__extract_year
          , subq_4.ds_partitioned__extract_quarter
          , subq_4.ds_partitioned__extract_month
          , subq_4.ds_partitioned__extract_day
          , subq_4.ds_partitioned__extract_dow
          , subq_4.ds_partitioned__extract_doy
          , subq_4.last_profile_edit_ts__millisecond
          , subq_4.last_profile_edit_ts__second
          , subq_4.last_profile_edit_ts__minute
          , subq_4.last_profile_edit_ts__hour
          , subq_4.last_profile_edit_ts__day
          , subq_4.last_profile_edit_ts__week
          , subq_4.last_profile_edit_ts__month
          , subq_4.last_profile_edit_ts__quarter
          , subq_4.last_profile_edit_ts__year
          , subq_4.last_profile_edit_ts__extract_year
          , subq_4.last_profile_edit_ts__extract_quarter
          , subq_4.last_profile_edit_ts__extract_month
          , subq_4.last_profile_edit_ts__extract_day
          , subq_4.last_profile_edit_ts__extract_dow
          , subq_4.last_profile_edit_ts__extract_doy
          , subq_4.bio_added_ts__second
          , subq_4.bio_added_ts__minute
          , subq_4.bio_added_ts__hour
          , subq_4.bio_added_ts__day
          , subq_4.bio_added_ts__week
          , subq_4.bio_added_ts__month
          , subq_4.bio_added_ts__quarter
          , subq_4.bio_added_ts__year
          , subq_4.bio_added_ts__extract_year
          , subq_4.bio_added_ts__extract_quarter
          , subq_4.bio_added_ts__extract_month
          , subq_4.bio_added_ts__extract_day
          , subq_4.bio_added_ts__extract_dow
          , subq_4.bio_added_ts__extract_doy
          , subq_4.last_login_ts__minute
          , subq_4.last_login_ts__hour
          , subq_4.last_login_ts__day
          , subq_4.last_login_ts__week
          , subq_4.last_login_ts__month
          , subq_4.last_login_ts__quarter
          , subq_4.last_login_ts__year
          , subq_4.last_login_ts__extract_year
          , subq_4.last_login_ts__extract_quarter
          , subq_4.last_login_ts__extract_month
          , subq_4.last_login_ts__extract_day
          , subq_4.last_login_ts__extract_dow
          , subq_4.last_login_ts__extract_doy
          , subq_4.archived_at__hour
          , subq_4.archived_at__day
          , subq_4.archived_at__week
          , subq_4.archived_at__month
          , subq_4.archived_at__quarter
          , subq_4.archived_at__year
          , subq_4.archived_at__extract_year
          , subq_4.archived_at__extract_quarter
          , subq_4.archived_at__extract_month
          , subq_4.archived_at__extract_day
          , subq_4.archived_at__extract_dow
          , subq_4.archived_at__extract_doy
          , subq_4.user__ds__day
          , subq_4.user__ds__week
          , subq_4.user__ds__month
          , subq_4.user__ds__quarter
          , subq_4.user__ds__year
          , subq_4.user__ds__extract_year
          , subq_4.user__ds__extract_quarter
          , subq_4.user__ds__extract_month
          , subq_4.user__ds__extract_day
          , subq_4.user__ds__extract_dow
          , subq_4.user__ds__extract_doy
          , subq_4.user__created_at__day
          , subq_4.user__created_at__week
          , subq_4.user__created_at__month
          , subq_4.user__created_at__quarter
          , subq_4.user__created_at__year
          , subq_4.user__created_at__extract_year
          , subq_4.user__created_at__extract_quarter
          , subq_4.user__created_at__extract_month
          , subq_4.user__created_at__extract_day
          , subq_4.user__created_at__extract_dow
          , subq_4.user__created_at__extract_doy
          , subq_4.user__ds_partitioned__day
          , subq_4.user__ds_partitioned__week
          , subq_4.user__ds_partitioned__month
          , subq_4.user__ds_partitioned__quarter
          , subq_4.user__ds_partitioned__year
          , subq_4.user__ds_partitioned__extract_year
          , subq_4.user__ds_partitioned__extract_quarter
          , subq_4.user__ds_partitioned__extract_month
          , subq_4.user__ds_partitioned__extract_day
          , subq_4.user__ds_partitioned__extract_dow
          , subq_4.user__ds_partitioned__extract_doy
          , subq_4.user__last_profile_edit_ts__millisecond
          , subq_4.user__last_profile_edit_ts__second
          , subq_4.user__last_profile_edit_ts__minute
          , subq_4.user__last_profile_edit_ts__hour
          , subq_4.user__last_profile_edit_ts__day
          , subq_4.user__last_profile_edit_ts__week
          , subq_4.user__last_profile_edit_ts__month
          , subq_4.user__last_profile_edit_ts__quarter
          , subq_4.user__last_profile_edit_ts__year
          , subq_4.user__last_profile_edit_ts__extract_year
          , subq_4.user__last_profile_edit_ts__extract_quarter
          , subq_4.user__last_profile_edit_ts__extract_month
          , subq_4.user__last_profile_edit_ts__extract_day
          , subq_4.user__last_profile_edit_ts__extract_dow
          , subq_4.user__last_profile_edit_ts__extract_doy
          , subq_4.user__bio_added_ts__second
          , subq_4.user__bio_added_ts__minute
          , subq_4.user__bio_added_ts__hour
          , subq_4.user__bio_added_ts__day
          , subq_4.user__bio_added_ts__week
          , subq_4.user__bio_added_ts__month
          , subq_4.user__bio_added_ts__quarter
          , subq_4.user__bio_added_ts__year
          , subq_4.user__bio_added_ts__extract_year
          , subq_4.user__bio_added_ts__extract_quarter
          , subq_4.user__bio_added_ts__extract_month
          , subq_4.user__bio_added_ts__extract_day
          , subq_4.user__bio_added_ts__extract_dow
          , subq_4.user__bio_added_ts__extract_doy
          , subq_4.user__last_login_ts__minute
          , subq_4.user__last_login_ts__hour
          , subq_4.user__last_login_ts__day
          , subq_4.user__last_login_ts__week
          , subq_4.user__last_login_ts__month
          , subq_4.user__last_login_ts__quarter
          , subq_4.user__last_login_ts__year
          , subq_4.user__last_login_ts__extract_year
          , subq_4.user__last_login_ts__extract_quarter
          , subq_4.user__last_login_ts__extract_month
          , subq_4.user__last_login_ts__extract_day
          , subq_4.user__last_login_ts__extract_dow
          , subq_4.user__last_login_ts__extract_doy
          , subq_4.user__archived_at__hour
          , subq_4.user__archived_at__day
          , subq_4.user__archived_at__week
          , subq_4.user__archived_at__month
          , subq_4.user__archived_at__quarter
          , subq_4.user__archived_at__year
          , subq_4.user__archived_at__extract_year
          , subq_4.user__archived_at__extract_quarter
          , subq_4.user__archived_at__extract_month
          , subq_4.user__archived_at__extract_day
          , subq_4.user__archived_at__extract_dow
          , subq_4.user__archived_at__extract_doy
          , subq_4.created_at__day AS metric_time__day
          , subq_4.created_at__week AS metric_time__week
          , subq_4.created_at__month AS metric_time__month
          , subq_4.created_at__quarter AS metric_time__quarter
          , subq_4.created_at__year AS metric_time__year
          , subq_4.created_at__extract_year AS metric_time__extract_year
          , subq_4.created_at__extract_quarter AS metric_time__extract_quarter
          , subq_4.created_at__extract_month AS metric_time__extract_month
          , subq_4.created_at__extract_day AS metric_time__extract_day
          , subq_4.created_at__extract_dow AS metric_time__extract_dow
          , subq_4.created_at__extract_doy AS metric_time__extract_doy
          , subq_4.user
          , subq_4.home_state
          , subq_4.user__home_state
        FROM (
          -- Read Elements From Semantic Model 'users_ds_source'
          SELECT
            1 AS new_users
            , 1 AS archived_users
            , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS ds__day
            , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS ds__week
            , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS ds__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS ds__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS ds__year
            , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS ds__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS ds__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS created_at__day
            , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS created_at__week
            , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS created_at__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS created_at__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS created_at__year
            , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS created_at__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS created_at__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS created_at__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS created_at__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS created_at__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
            , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__week
            , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
            , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
            , users_ds_source_src_28000.home_state
            , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
            , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
            , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
            , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
            , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__week
            , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
            , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
            , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
            , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
            , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
            , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__week
            , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
            , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
            , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
            , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
            , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS last_login_ts__week
            , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
            , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
            , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS archived_at__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS archived_at__day
            , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS archived_at__week
            , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS archived_at__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS archived_at__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS archived_at__year
            , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS user__ds__day
            , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS user__ds__week
            , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS user__ds__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS user__ds__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS user__ds__year
            , EXTRACT(year FROM users_ds_source_src_28000.ds) AS user__ds__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS user__ds__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.ds) AS user__ds__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.ds) AS user__ds__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS user__ds__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS user__created_at__day
            , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS user__created_at__week
            , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS user__created_at__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS user__created_at__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS user__created_at__year
            , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
            , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__week
            , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
            , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
            , users_ds_source_src_28000.home_state AS user__home_state
            , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
            , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
            , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
            , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
            , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__week
            , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
            , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
            , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
            , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
            , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
            , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__week
            , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
            , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
            , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
            , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
            , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__week
            , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
            , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
            , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS user__archived_at__hour
            , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS user__archived_at__day
            , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS user__archived_at__week
            , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS user__archived_at__month
            , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
            , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS user__archived_at__year
            , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
            , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
            , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
            , users_ds_source_src_28000.user_id AS user
          FROM ***************************.dim_users users_ds_source_src_28000
        ) subq_4
      ) subq_5
      ON
        subq_3.user = subq_5.user
    ) subq_6
    ON
      (
        subq_1.listing = subq_6.listing
      ) AND (
        subq_1.ds_partitioned__day = subq_6.ds_partitioned__day
      )
  ) subq_7
  GROUP BY
    subq_7.listing__user__bio_added_ts__minute
) subq_8
