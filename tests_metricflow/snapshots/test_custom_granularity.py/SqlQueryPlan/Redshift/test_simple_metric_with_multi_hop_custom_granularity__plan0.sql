-- Compute Metrics via Expressions
SELECT
  subq_14.listing__user__ds__martian_day
  , subq_14.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_13.listing__user__ds__martian_day
    , SUM(subq_13.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'listing__user__ds__martian_day']
    SELECT
      subq_12.listing__user__ds__martian_day
      , subq_12.bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'listing__user__ds__day']
      -- Join to Custom Granularity Dataset
      SELECT
        subq_10.listing__user__ds__day AS listing__user__ds__day
        , subq_10.bookings AS bookings
        , subq_11.martian_day AS listing__user__ds__martian_day
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_9.user__ds__day AS listing__user__ds__day
          , subq_9.user__ds_partitioned__day AS listing__user__ds_partitioned__day
          , subq_2.ds_partitioned__day AS ds_partitioned__day
          , subq_2.listing AS listing
          , subq_2.bookings AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'ds_partitioned__day', 'listing']
          SELECT
            subq_1.ds_partitioned__day
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
          -- Pass Only Elements: ['user__ds_partitioned__day', 'user__ds__day', 'listing']
          SELECT
            subq_8.user__ds__day
            , subq_8.user__ds_partitioned__day
            , subq_8.listing
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_7.home_state AS user__home_state
              , subq_7.ds__day AS user__ds__day
              , subq_7.ds__week AS user__ds__week
              , subq_7.ds__month AS user__ds__month
              , subq_7.ds__quarter AS user__ds__quarter
              , subq_7.ds__year AS user__ds__year
              , subq_7.ds__extract_year AS user__ds__extract_year
              , subq_7.ds__extract_quarter AS user__ds__extract_quarter
              , subq_7.ds__extract_month AS user__ds__extract_month
              , subq_7.ds__extract_day AS user__ds__extract_day
              , subq_7.ds__extract_dow AS user__ds__extract_dow
              , subq_7.ds__extract_doy AS user__ds__extract_doy
              , subq_7.created_at__day AS user__created_at__day
              , subq_7.created_at__week AS user__created_at__week
              , subq_7.created_at__month AS user__created_at__month
              , subq_7.created_at__quarter AS user__created_at__quarter
              , subq_7.created_at__year AS user__created_at__year
              , subq_7.created_at__extract_year AS user__created_at__extract_year
              , subq_7.created_at__extract_quarter AS user__created_at__extract_quarter
              , subq_7.created_at__extract_month AS user__created_at__extract_month
              , subq_7.created_at__extract_day AS user__created_at__extract_day
              , subq_7.created_at__extract_dow AS user__created_at__extract_dow
              , subq_7.created_at__extract_doy AS user__created_at__extract_doy
              , subq_7.ds_partitioned__day AS user__ds_partitioned__day
              , subq_7.ds_partitioned__week AS user__ds_partitioned__week
              , subq_7.ds_partitioned__month AS user__ds_partitioned__month
              , subq_7.ds_partitioned__quarter AS user__ds_partitioned__quarter
              , subq_7.ds_partitioned__year AS user__ds_partitioned__year
              , subq_7.ds_partitioned__extract_year AS user__ds_partitioned__extract_year
              , subq_7.ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
              , subq_7.ds_partitioned__extract_month AS user__ds_partitioned__extract_month
              , subq_7.ds_partitioned__extract_day AS user__ds_partitioned__extract_day
              , subq_7.ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
              , subq_7.ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
              , subq_7.last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
              , subq_7.last_profile_edit_ts__second AS user__last_profile_edit_ts__second
              , subq_7.last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
              , subq_7.last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
              , subq_7.last_profile_edit_ts__day AS user__last_profile_edit_ts__day
              , subq_7.last_profile_edit_ts__week AS user__last_profile_edit_ts__week
              , subq_7.last_profile_edit_ts__month AS user__last_profile_edit_ts__month
              , subq_7.last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
              , subq_7.last_profile_edit_ts__year AS user__last_profile_edit_ts__year
              , subq_7.last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
              , subq_7.last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
              , subq_7.last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
              , subq_7.last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
              , subq_7.last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
              , subq_7.last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
              , subq_7.bio_added_ts__second AS user__bio_added_ts__second
              , subq_7.bio_added_ts__minute AS user__bio_added_ts__minute
              , subq_7.bio_added_ts__hour AS user__bio_added_ts__hour
              , subq_7.bio_added_ts__day AS user__bio_added_ts__day
              , subq_7.bio_added_ts__week AS user__bio_added_ts__week
              , subq_7.bio_added_ts__month AS user__bio_added_ts__month
              , subq_7.bio_added_ts__quarter AS user__bio_added_ts__quarter
              , subq_7.bio_added_ts__year AS user__bio_added_ts__year
              , subq_7.bio_added_ts__extract_year AS user__bio_added_ts__extract_year
              , subq_7.bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
              , subq_7.bio_added_ts__extract_month AS user__bio_added_ts__extract_month
              , subq_7.bio_added_ts__extract_day AS user__bio_added_ts__extract_day
              , subq_7.bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
              , subq_7.bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
              , subq_7.last_login_ts__minute AS user__last_login_ts__minute
              , subq_7.last_login_ts__hour AS user__last_login_ts__hour
              , subq_7.last_login_ts__day AS user__last_login_ts__day
              , subq_7.last_login_ts__week AS user__last_login_ts__week
              , subq_7.last_login_ts__month AS user__last_login_ts__month
              , subq_7.last_login_ts__quarter AS user__last_login_ts__quarter
              , subq_7.last_login_ts__year AS user__last_login_ts__year
              , subq_7.last_login_ts__extract_year AS user__last_login_ts__extract_year
              , subq_7.last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
              , subq_7.last_login_ts__extract_month AS user__last_login_ts__extract_month
              , subq_7.last_login_ts__extract_day AS user__last_login_ts__extract_day
              , subq_7.last_login_ts__extract_dow AS user__last_login_ts__extract_dow
              , subq_7.last_login_ts__extract_doy AS user__last_login_ts__extract_doy
              , subq_7.archived_at__hour AS user__archived_at__hour
              , subq_7.archived_at__day AS user__archived_at__day
              , subq_7.archived_at__week AS user__archived_at__week
              , subq_7.archived_at__month AS user__archived_at__month
              , subq_7.archived_at__quarter AS user__archived_at__quarter
              , subq_7.archived_at__year AS user__archived_at__year
              , subq_7.archived_at__extract_year AS user__archived_at__extract_year
              , subq_7.archived_at__extract_quarter AS user__archived_at__extract_quarter
              , subq_7.archived_at__extract_month AS user__archived_at__extract_month
              , subq_7.archived_at__extract_day AS user__archived_at__extract_day
              , subq_7.archived_at__extract_dow AS user__archived_at__extract_dow
              , subq_7.archived_at__extract_doy AS user__archived_at__extract_doy
              , subq_7.metric_time__day AS user__metric_time__day
              , subq_7.metric_time__week AS user__metric_time__week
              , subq_7.metric_time__month AS user__metric_time__month
              , subq_7.metric_time__quarter AS user__metric_time__quarter
              , subq_7.metric_time__year AS user__metric_time__year
              , subq_7.metric_time__extract_year AS user__metric_time__extract_year
              , subq_7.metric_time__extract_quarter AS user__metric_time__extract_quarter
              , subq_7.metric_time__extract_month AS user__metric_time__extract_month
              , subq_7.metric_time__extract_day AS user__metric_time__extract_day
              , subq_7.metric_time__extract_dow AS user__metric_time__extract_dow
              , subq_7.metric_time__extract_doy AS user__metric_time__extract_doy
              , subq_4.ds__day AS ds__day
              , subq_4.ds__week AS ds__week
              , subq_4.ds__month AS ds__month
              , subq_4.ds__quarter AS ds__quarter
              , subq_4.ds__year AS ds__year
              , subq_4.ds__extract_year AS ds__extract_year
              , subq_4.ds__extract_quarter AS ds__extract_quarter
              , subq_4.ds__extract_month AS ds__extract_month
              , subq_4.ds__extract_day AS ds__extract_day
              , subq_4.ds__extract_dow AS ds__extract_dow
              , subq_4.ds__extract_doy AS ds__extract_doy
              , subq_4.created_at__day AS created_at__day
              , subq_4.created_at__week AS created_at__week
              , subq_4.created_at__month AS created_at__month
              , subq_4.created_at__quarter AS created_at__quarter
              , subq_4.created_at__year AS created_at__year
              , subq_4.created_at__extract_year AS created_at__extract_year
              , subq_4.created_at__extract_quarter AS created_at__extract_quarter
              , subq_4.created_at__extract_month AS created_at__extract_month
              , subq_4.created_at__extract_day AS created_at__extract_day
              , subq_4.created_at__extract_dow AS created_at__extract_dow
              , subq_4.created_at__extract_doy AS created_at__extract_doy
              , subq_4.listing__ds__day AS listing__ds__day
              , subq_4.listing__ds__week AS listing__ds__week
              , subq_4.listing__ds__month AS listing__ds__month
              , subq_4.listing__ds__quarter AS listing__ds__quarter
              , subq_4.listing__ds__year AS listing__ds__year
              , subq_4.listing__ds__extract_year AS listing__ds__extract_year
              , subq_4.listing__ds__extract_quarter AS listing__ds__extract_quarter
              , subq_4.listing__ds__extract_month AS listing__ds__extract_month
              , subq_4.listing__ds__extract_day AS listing__ds__extract_day
              , subq_4.listing__ds__extract_dow AS listing__ds__extract_dow
              , subq_4.listing__ds__extract_doy AS listing__ds__extract_doy
              , subq_4.listing__created_at__day AS listing__created_at__day
              , subq_4.listing__created_at__week AS listing__created_at__week
              , subq_4.listing__created_at__month AS listing__created_at__month
              , subq_4.listing__created_at__quarter AS listing__created_at__quarter
              , subq_4.listing__created_at__year AS listing__created_at__year
              , subq_4.listing__created_at__extract_year AS listing__created_at__extract_year
              , subq_4.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
              , subq_4.listing__created_at__extract_month AS listing__created_at__extract_month
              , subq_4.listing__created_at__extract_day AS listing__created_at__extract_day
              , subq_4.listing__created_at__extract_dow AS listing__created_at__extract_dow
              , subq_4.listing__created_at__extract_doy AS listing__created_at__extract_doy
              , subq_4.metric_time__day AS metric_time__day
              , subq_4.metric_time__week AS metric_time__week
              , subq_4.metric_time__month AS metric_time__month
              , subq_4.metric_time__quarter AS metric_time__quarter
              , subq_4.metric_time__year AS metric_time__year
              , subq_4.metric_time__extract_year AS metric_time__extract_year
              , subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_4.metric_time__extract_month AS metric_time__extract_month
              , subq_4.metric_time__extract_day AS metric_time__extract_day
              , subq_4.metric_time__extract_dow AS metric_time__extract_dow
              , subq_4.metric_time__extract_doy AS metric_time__extract_doy
              , subq_4.listing AS listing
              , subq_4.user AS user
              , subq_4.listing__user AS listing__user
              , subq_4.country_latest AS country_latest
              , subq_4.is_lux_latest AS is_lux_latest
              , subq_4.capacity_latest AS capacity_latest
              , subq_4.listing__country_latest AS listing__country_latest
              , subq_4.listing__is_lux_latest AS listing__is_lux_latest
              , subq_4.listing__capacity_latest AS listing__capacity_latest
              , subq_4.listings AS listings
              , subq_4.largest_listing AS largest_listing
              , subq_4.smallest_listing AS smallest_listing
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
            LEFT OUTER JOIN (
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
                subq_6.ds__day
                , subq_6.ds__week
                , subq_6.ds__month
                , subq_6.ds__quarter
                , subq_6.ds__year
                , subq_6.ds__extract_year
                , subq_6.ds__extract_quarter
                , subq_6.ds__extract_month
                , subq_6.ds__extract_day
                , subq_6.ds__extract_dow
                , subq_6.ds__extract_doy
                , subq_6.created_at__day
                , subq_6.created_at__week
                , subq_6.created_at__month
                , subq_6.created_at__quarter
                , subq_6.created_at__year
                , subq_6.created_at__extract_year
                , subq_6.created_at__extract_quarter
                , subq_6.created_at__extract_month
                , subq_6.created_at__extract_day
                , subq_6.created_at__extract_dow
                , subq_6.created_at__extract_doy
                , subq_6.ds_partitioned__day
                , subq_6.ds_partitioned__week
                , subq_6.ds_partitioned__month
                , subq_6.ds_partitioned__quarter
                , subq_6.ds_partitioned__year
                , subq_6.ds_partitioned__extract_year
                , subq_6.ds_partitioned__extract_quarter
                , subq_6.ds_partitioned__extract_month
                , subq_6.ds_partitioned__extract_day
                , subq_6.ds_partitioned__extract_dow
                , subq_6.ds_partitioned__extract_doy
                , subq_6.last_profile_edit_ts__millisecond
                , subq_6.last_profile_edit_ts__second
                , subq_6.last_profile_edit_ts__minute
                , subq_6.last_profile_edit_ts__hour
                , subq_6.last_profile_edit_ts__day
                , subq_6.last_profile_edit_ts__week
                , subq_6.last_profile_edit_ts__month
                , subq_6.last_profile_edit_ts__quarter
                , subq_6.last_profile_edit_ts__year
                , subq_6.last_profile_edit_ts__extract_year
                , subq_6.last_profile_edit_ts__extract_quarter
                , subq_6.last_profile_edit_ts__extract_month
                , subq_6.last_profile_edit_ts__extract_day
                , subq_6.last_profile_edit_ts__extract_dow
                , subq_6.last_profile_edit_ts__extract_doy
                , subq_6.bio_added_ts__second
                , subq_6.bio_added_ts__minute
                , subq_6.bio_added_ts__hour
                , subq_6.bio_added_ts__day
                , subq_6.bio_added_ts__week
                , subq_6.bio_added_ts__month
                , subq_6.bio_added_ts__quarter
                , subq_6.bio_added_ts__year
                , subq_6.bio_added_ts__extract_year
                , subq_6.bio_added_ts__extract_quarter
                , subq_6.bio_added_ts__extract_month
                , subq_6.bio_added_ts__extract_day
                , subq_6.bio_added_ts__extract_dow
                , subq_6.bio_added_ts__extract_doy
                , subq_6.last_login_ts__minute
                , subq_6.last_login_ts__hour
                , subq_6.last_login_ts__day
                , subq_6.last_login_ts__week
                , subq_6.last_login_ts__month
                , subq_6.last_login_ts__quarter
                , subq_6.last_login_ts__year
                , subq_6.last_login_ts__extract_year
                , subq_6.last_login_ts__extract_quarter
                , subq_6.last_login_ts__extract_month
                , subq_6.last_login_ts__extract_day
                , subq_6.last_login_ts__extract_dow
                , subq_6.last_login_ts__extract_doy
                , subq_6.archived_at__hour
                , subq_6.archived_at__day
                , subq_6.archived_at__week
                , subq_6.archived_at__month
                , subq_6.archived_at__quarter
                , subq_6.archived_at__year
                , subq_6.archived_at__extract_year
                , subq_6.archived_at__extract_quarter
                , subq_6.archived_at__extract_month
                , subq_6.archived_at__extract_day
                , subq_6.archived_at__extract_dow
                , subq_6.archived_at__extract_doy
                , subq_6.user__ds__day
                , subq_6.user__ds__week
                , subq_6.user__ds__month
                , subq_6.user__ds__quarter
                , subq_6.user__ds__year
                , subq_6.user__ds__extract_year
                , subq_6.user__ds__extract_quarter
                , subq_6.user__ds__extract_month
                , subq_6.user__ds__extract_day
                , subq_6.user__ds__extract_dow
                , subq_6.user__ds__extract_doy
                , subq_6.user__created_at__day
                , subq_6.user__created_at__week
                , subq_6.user__created_at__month
                , subq_6.user__created_at__quarter
                , subq_6.user__created_at__year
                , subq_6.user__created_at__extract_year
                , subq_6.user__created_at__extract_quarter
                , subq_6.user__created_at__extract_month
                , subq_6.user__created_at__extract_day
                , subq_6.user__created_at__extract_dow
                , subq_6.user__created_at__extract_doy
                , subq_6.user__ds_partitioned__day
                , subq_6.user__ds_partitioned__week
                , subq_6.user__ds_partitioned__month
                , subq_6.user__ds_partitioned__quarter
                , subq_6.user__ds_partitioned__year
                , subq_6.user__ds_partitioned__extract_year
                , subq_6.user__ds_partitioned__extract_quarter
                , subq_6.user__ds_partitioned__extract_month
                , subq_6.user__ds_partitioned__extract_day
                , subq_6.user__ds_partitioned__extract_dow
                , subq_6.user__ds_partitioned__extract_doy
                , subq_6.user__last_profile_edit_ts__millisecond
                , subq_6.user__last_profile_edit_ts__second
                , subq_6.user__last_profile_edit_ts__minute
                , subq_6.user__last_profile_edit_ts__hour
                , subq_6.user__last_profile_edit_ts__day
                , subq_6.user__last_profile_edit_ts__week
                , subq_6.user__last_profile_edit_ts__month
                , subq_6.user__last_profile_edit_ts__quarter
                , subq_6.user__last_profile_edit_ts__year
                , subq_6.user__last_profile_edit_ts__extract_year
                , subq_6.user__last_profile_edit_ts__extract_quarter
                , subq_6.user__last_profile_edit_ts__extract_month
                , subq_6.user__last_profile_edit_ts__extract_day
                , subq_6.user__last_profile_edit_ts__extract_dow
                , subq_6.user__last_profile_edit_ts__extract_doy
                , subq_6.user__bio_added_ts__second
                , subq_6.user__bio_added_ts__minute
                , subq_6.user__bio_added_ts__hour
                , subq_6.user__bio_added_ts__day
                , subq_6.user__bio_added_ts__week
                , subq_6.user__bio_added_ts__month
                , subq_6.user__bio_added_ts__quarter
                , subq_6.user__bio_added_ts__year
                , subq_6.user__bio_added_ts__extract_year
                , subq_6.user__bio_added_ts__extract_quarter
                , subq_6.user__bio_added_ts__extract_month
                , subq_6.user__bio_added_ts__extract_day
                , subq_6.user__bio_added_ts__extract_dow
                , subq_6.user__bio_added_ts__extract_doy
                , subq_6.user__last_login_ts__minute
                , subq_6.user__last_login_ts__hour
                , subq_6.user__last_login_ts__day
                , subq_6.user__last_login_ts__week
                , subq_6.user__last_login_ts__month
                , subq_6.user__last_login_ts__quarter
                , subq_6.user__last_login_ts__year
                , subq_6.user__last_login_ts__extract_year
                , subq_6.user__last_login_ts__extract_quarter
                , subq_6.user__last_login_ts__extract_month
                , subq_6.user__last_login_ts__extract_day
                , subq_6.user__last_login_ts__extract_dow
                , subq_6.user__last_login_ts__extract_doy
                , subq_6.user__archived_at__hour
                , subq_6.user__archived_at__day
                , subq_6.user__archived_at__week
                , subq_6.user__archived_at__month
                , subq_6.user__archived_at__quarter
                , subq_6.user__archived_at__year
                , subq_6.user__archived_at__extract_year
                , subq_6.user__archived_at__extract_quarter
                , subq_6.user__archived_at__extract_month
                , subq_6.user__archived_at__extract_day
                , subq_6.user__archived_at__extract_dow
                , subq_6.user__archived_at__extract_doy
                , subq_6.metric_time__day
                , subq_6.metric_time__week
                , subq_6.metric_time__month
                , subq_6.metric_time__quarter
                , subq_6.metric_time__year
                , subq_6.metric_time__extract_year
                , subq_6.metric_time__extract_quarter
                , subq_6.metric_time__extract_month
                , subq_6.metric_time__extract_day
                , subq_6.metric_time__extract_dow
                , subq_6.metric_time__extract_doy
                , subq_6.user
                , subq_6.home_state
                , subq_6.user__home_state
              FROM (
                -- Metric Time Dimension 'created_at'
                SELECT
                  subq_5.ds__day
                  , subq_5.ds__week
                  , subq_5.ds__month
                  , subq_5.ds__quarter
                  , subq_5.ds__year
                  , subq_5.ds__extract_year
                  , subq_5.ds__extract_quarter
                  , subq_5.ds__extract_month
                  , subq_5.ds__extract_day
                  , subq_5.ds__extract_dow
                  , subq_5.ds__extract_doy
                  , subq_5.created_at__day
                  , subq_5.created_at__week
                  , subq_5.created_at__month
                  , subq_5.created_at__quarter
                  , subq_5.created_at__year
                  , subq_5.created_at__extract_year
                  , subq_5.created_at__extract_quarter
                  , subq_5.created_at__extract_month
                  , subq_5.created_at__extract_day
                  , subq_5.created_at__extract_dow
                  , subq_5.created_at__extract_doy
                  , subq_5.ds_partitioned__day
                  , subq_5.ds_partitioned__week
                  , subq_5.ds_partitioned__month
                  , subq_5.ds_partitioned__quarter
                  , subq_5.ds_partitioned__year
                  , subq_5.ds_partitioned__extract_year
                  , subq_5.ds_partitioned__extract_quarter
                  , subq_5.ds_partitioned__extract_month
                  , subq_5.ds_partitioned__extract_day
                  , subq_5.ds_partitioned__extract_dow
                  , subq_5.ds_partitioned__extract_doy
                  , subq_5.last_profile_edit_ts__millisecond
                  , subq_5.last_profile_edit_ts__second
                  , subq_5.last_profile_edit_ts__minute
                  , subq_5.last_profile_edit_ts__hour
                  , subq_5.last_profile_edit_ts__day
                  , subq_5.last_profile_edit_ts__week
                  , subq_5.last_profile_edit_ts__month
                  , subq_5.last_profile_edit_ts__quarter
                  , subq_5.last_profile_edit_ts__year
                  , subq_5.last_profile_edit_ts__extract_year
                  , subq_5.last_profile_edit_ts__extract_quarter
                  , subq_5.last_profile_edit_ts__extract_month
                  , subq_5.last_profile_edit_ts__extract_day
                  , subq_5.last_profile_edit_ts__extract_dow
                  , subq_5.last_profile_edit_ts__extract_doy
                  , subq_5.bio_added_ts__second
                  , subq_5.bio_added_ts__minute
                  , subq_5.bio_added_ts__hour
                  , subq_5.bio_added_ts__day
                  , subq_5.bio_added_ts__week
                  , subq_5.bio_added_ts__month
                  , subq_5.bio_added_ts__quarter
                  , subq_5.bio_added_ts__year
                  , subq_5.bio_added_ts__extract_year
                  , subq_5.bio_added_ts__extract_quarter
                  , subq_5.bio_added_ts__extract_month
                  , subq_5.bio_added_ts__extract_day
                  , subq_5.bio_added_ts__extract_dow
                  , subq_5.bio_added_ts__extract_doy
                  , subq_5.last_login_ts__minute
                  , subq_5.last_login_ts__hour
                  , subq_5.last_login_ts__day
                  , subq_5.last_login_ts__week
                  , subq_5.last_login_ts__month
                  , subq_5.last_login_ts__quarter
                  , subq_5.last_login_ts__year
                  , subq_5.last_login_ts__extract_year
                  , subq_5.last_login_ts__extract_quarter
                  , subq_5.last_login_ts__extract_month
                  , subq_5.last_login_ts__extract_day
                  , subq_5.last_login_ts__extract_dow
                  , subq_5.last_login_ts__extract_doy
                  , subq_5.archived_at__hour
                  , subq_5.archived_at__day
                  , subq_5.archived_at__week
                  , subq_5.archived_at__month
                  , subq_5.archived_at__quarter
                  , subq_5.archived_at__year
                  , subq_5.archived_at__extract_year
                  , subq_5.archived_at__extract_quarter
                  , subq_5.archived_at__extract_month
                  , subq_5.archived_at__extract_day
                  , subq_5.archived_at__extract_dow
                  , subq_5.archived_at__extract_doy
                  , subq_5.user__ds__day
                  , subq_5.user__ds__week
                  , subq_5.user__ds__month
                  , subq_5.user__ds__quarter
                  , subq_5.user__ds__year
                  , subq_5.user__ds__extract_year
                  , subq_5.user__ds__extract_quarter
                  , subq_5.user__ds__extract_month
                  , subq_5.user__ds__extract_day
                  , subq_5.user__ds__extract_dow
                  , subq_5.user__ds__extract_doy
                  , subq_5.user__created_at__day
                  , subq_5.user__created_at__week
                  , subq_5.user__created_at__month
                  , subq_5.user__created_at__quarter
                  , subq_5.user__created_at__year
                  , subq_5.user__created_at__extract_year
                  , subq_5.user__created_at__extract_quarter
                  , subq_5.user__created_at__extract_month
                  , subq_5.user__created_at__extract_day
                  , subq_5.user__created_at__extract_dow
                  , subq_5.user__created_at__extract_doy
                  , subq_5.user__ds_partitioned__day
                  , subq_5.user__ds_partitioned__week
                  , subq_5.user__ds_partitioned__month
                  , subq_5.user__ds_partitioned__quarter
                  , subq_5.user__ds_partitioned__year
                  , subq_5.user__ds_partitioned__extract_year
                  , subq_5.user__ds_partitioned__extract_quarter
                  , subq_5.user__ds_partitioned__extract_month
                  , subq_5.user__ds_partitioned__extract_day
                  , subq_5.user__ds_partitioned__extract_dow
                  , subq_5.user__ds_partitioned__extract_doy
                  , subq_5.user__last_profile_edit_ts__millisecond
                  , subq_5.user__last_profile_edit_ts__second
                  , subq_5.user__last_profile_edit_ts__minute
                  , subq_5.user__last_profile_edit_ts__hour
                  , subq_5.user__last_profile_edit_ts__day
                  , subq_5.user__last_profile_edit_ts__week
                  , subq_5.user__last_profile_edit_ts__month
                  , subq_5.user__last_profile_edit_ts__quarter
                  , subq_5.user__last_profile_edit_ts__year
                  , subq_5.user__last_profile_edit_ts__extract_year
                  , subq_5.user__last_profile_edit_ts__extract_quarter
                  , subq_5.user__last_profile_edit_ts__extract_month
                  , subq_5.user__last_profile_edit_ts__extract_day
                  , subq_5.user__last_profile_edit_ts__extract_dow
                  , subq_5.user__last_profile_edit_ts__extract_doy
                  , subq_5.user__bio_added_ts__second
                  , subq_5.user__bio_added_ts__minute
                  , subq_5.user__bio_added_ts__hour
                  , subq_5.user__bio_added_ts__day
                  , subq_5.user__bio_added_ts__week
                  , subq_5.user__bio_added_ts__month
                  , subq_5.user__bio_added_ts__quarter
                  , subq_5.user__bio_added_ts__year
                  , subq_5.user__bio_added_ts__extract_year
                  , subq_5.user__bio_added_ts__extract_quarter
                  , subq_5.user__bio_added_ts__extract_month
                  , subq_5.user__bio_added_ts__extract_day
                  , subq_5.user__bio_added_ts__extract_dow
                  , subq_5.user__bio_added_ts__extract_doy
                  , subq_5.user__last_login_ts__minute
                  , subq_5.user__last_login_ts__hour
                  , subq_5.user__last_login_ts__day
                  , subq_5.user__last_login_ts__week
                  , subq_5.user__last_login_ts__month
                  , subq_5.user__last_login_ts__quarter
                  , subq_5.user__last_login_ts__year
                  , subq_5.user__last_login_ts__extract_year
                  , subq_5.user__last_login_ts__extract_quarter
                  , subq_5.user__last_login_ts__extract_month
                  , subq_5.user__last_login_ts__extract_day
                  , subq_5.user__last_login_ts__extract_dow
                  , subq_5.user__last_login_ts__extract_doy
                  , subq_5.user__archived_at__hour
                  , subq_5.user__archived_at__day
                  , subq_5.user__archived_at__week
                  , subq_5.user__archived_at__month
                  , subq_5.user__archived_at__quarter
                  , subq_5.user__archived_at__year
                  , subq_5.user__archived_at__extract_year
                  , subq_5.user__archived_at__extract_quarter
                  , subq_5.user__archived_at__extract_month
                  , subq_5.user__archived_at__extract_day
                  , subq_5.user__archived_at__extract_dow
                  , subq_5.user__archived_at__extract_doy
                  , subq_5.created_at__day AS metric_time__day
                  , subq_5.created_at__week AS metric_time__week
                  , subq_5.created_at__month AS metric_time__month
                  , subq_5.created_at__quarter AS metric_time__quarter
                  , subq_5.created_at__year AS metric_time__year
                  , subq_5.created_at__extract_year AS metric_time__extract_year
                  , subq_5.created_at__extract_quarter AS metric_time__extract_quarter
                  , subq_5.created_at__extract_month AS metric_time__extract_month
                  , subq_5.created_at__extract_day AS metric_time__extract_day
                  , subq_5.created_at__extract_dow AS metric_time__extract_dow
                  , subq_5.created_at__extract_doy AS metric_time__extract_doy
                  , subq_5.user
                  , subq_5.home_state
                  , subq_5.user__home_state
                  , subq_5.new_users
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds) END AS ds__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.created_at) END AS created_at__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) END AS last_profile_edit_ts__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) END AS bio_added_ts__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) END AS last_login_ts__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.archived_at) END AS archived_at__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds) END AS user__ds__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.created_at) END AS user__created_at__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) END AS user__ds_partitioned__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) END AS user__last_profile_edit_ts__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) END AS user__bio_added_ts__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) END AS user__last_login_ts__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.archived_at) END AS user__archived_at__extract_dow
                    , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
                    , users_ds_source_src_28000.user_id AS user
                  FROM ***************************.dim_users users_ds_source_src_28000
                ) subq_5
              ) subq_6
            ) subq_7
            ON
              subq_4.user = subq_7.user
          ) subq_8
        ) subq_9
        ON
          (
            subq_2.listing = subq_9.listing
          ) AND (
            subq_2.ds_partitioned__day = subq_9.user__ds_partitioned__day
          )
      ) subq_10
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_11
      ON
        subq_10.listing__user__ds__day = subq_11.ds
    ) subq_12
  ) subq_13
  GROUP BY
    subq_13.listing__user__ds__martian_day
) subq_14
