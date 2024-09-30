-- Compute Metrics via Expressions
SELECT
  subq_6.metric_time__day
  , subq_6.listing__user__home_state_latest
  , subq_6.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_5.metric_time__day
    , subq_5.listing__user__home_state_latest
    , SUM(subq_5.bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__user__home_state_latest', 'metric_time__day']
    SELECT
      subq_1.metric_time__day AS metric_time__day
      , subq_1.bookings AS bookings
    FROM (
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
      SELECT
        subq_0.ds__day AS metric_time__day
        , subq_0.listing
        , subq_0.bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS bookings
          , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
          , bookings_source_src_26000.booking_value
          , bookings_source_src_26000.guest_id AS bookers
          , bookings_source_src_26000.booking_value AS average_booking_value
          , bookings_source_src_26000.booking_value AS booking_payments
          , bookings_source_src_26000.is_instant
          , DATE_TRUNC('day', bookings_source_src_26000.ds) AS ds__day
          , DATE_TRUNC('week', bookings_source_src_26000.ds) AS ds__week
          , DATE_TRUNC('month', bookings_source_src_26000.ds) AS ds__month
          , DATE_TRUNC('quarter', bookings_source_src_26000.ds) AS ds__quarter
          , DATE_TRUNC('year', bookings_source_src_26000.ds) AS ds__year
          , EXTRACT(year FROM bookings_source_src_26000.ds) AS ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_26000.ds) AS ds__extract_month
          , EXTRACT(day FROM bookings_source_src_26000.ds) AS ds__extract_day
          , EXTRACT(isodow FROM bookings_source_src_26000.ds) AS ds__extract_dow
          , EXTRACT(doy FROM bookings_source_src_26000.ds) AS ds__extract_doy
          , DATE_TRUNC('day', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__day
          , DATE_TRUNC('week', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_day
          , EXTRACT(isodow FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_dow
          , EXTRACT(doy FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_doy
          , DATE_TRUNC('day', bookings_source_src_26000.paid_at) AS paid_at__day
          , DATE_TRUNC('week', bookings_source_src_26000.paid_at) AS paid_at__week
          , DATE_TRUNC('month', bookings_source_src_26000.paid_at) AS paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_26000.paid_at) AS paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_26000.paid_at) AS paid_at__year
          , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS paid_at__extract_day
          , EXTRACT(isodow FROM bookings_source_src_26000.paid_at) AS paid_at__extract_dow
          , EXTRACT(doy FROM bookings_source_src_26000.paid_at) AS paid_at__extract_doy
          , bookings_source_src_26000.is_instant AS booking__is_instant
          , DATE_TRUNC('day', bookings_source_src_26000.ds) AS booking__ds__day
          , DATE_TRUNC('week', bookings_source_src_26000.ds) AS booking__ds__week
          , DATE_TRUNC('month', bookings_source_src_26000.ds) AS booking__ds__month
          , DATE_TRUNC('quarter', bookings_source_src_26000.ds) AS booking__ds__quarter
          , DATE_TRUNC('year', bookings_source_src_26000.ds) AS booking__ds__year
          , EXTRACT(year FROM bookings_source_src_26000.ds) AS booking__ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS booking__ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_26000.ds) AS booking__ds__extract_month
          , EXTRACT(day FROM bookings_source_src_26000.ds) AS booking__ds__extract_day
          , EXTRACT(isodow FROM bookings_source_src_26000.ds) AS booking__ds__extract_dow
          , EXTRACT(doy FROM bookings_source_src_26000.ds) AS booking__ds__extract_doy
          , DATE_TRUNC('day', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__day
          , DATE_TRUNC('week', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_day
          , EXTRACT(isodow FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_dow
          , EXTRACT(doy FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_doy
          , DATE_TRUNC('day', bookings_source_src_26000.paid_at) AS booking__paid_at__day
          , DATE_TRUNC('week', bookings_source_src_26000.paid_at) AS booking__paid_at__week
          , DATE_TRUNC('month', bookings_source_src_26000.paid_at) AS booking__paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_26000.paid_at) AS booking__paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_26000.paid_at) AS booking__paid_at__year
          , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_day
          , EXTRACT(isodow FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_dow
          , EXTRACT(doy FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_doy
          , bookings_source_src_26000.listing_id AS listing
          , bookings_source_src_26000.guest_id AS guest
          , bookings_source_src_26000.host_id AS host
          , bookings_source_src_26000.guest_id AS user
          , bookings_source_src_26000.listing_id AS booking__listing
          , bookings_source_src_26000.guest_id AS booking__guest
          , bookings_source_src_26000.host_id AS booking__host
          , bookings_source_src_26000.guest_id AS booking__user
        FROM ***************************.fct_bookings bookings_source_src_26000
      ) subq_0
    ) subq_1
    LEFT OUTER JOIN (
      -- Join Standard Outputs
      -- Pass Only Elements: ['user__home_state_latest', 'window_start__day', 'window_end__day', 'listing']
      SELECT
        subq_2.window_start__day AS window_start__day
        , subq_2.window_end__day AS window_end__day
        , subq_2.listing AS listing
      FROM (
        -- Read Elements From Semantic Model 'listings'
        SELECT
          listings_src_26000.active_from AS window_start__day
          , DATE_TRUNC('week', listings_src_26000.active_from) AS window_start__week
          , DATE_TRUNC('month', listings_src_26000.active_from) AS window_start__month
          , DATE_TRUNC('quarter', listings_src_26000.active_from) AS window_start__quarter
          , DATE_TRUNC('year', listings_src_26000.active_from) AS window_start__year
          , EXTRACT(year FROM listings_src_26000.active_from) AS window_start__extract_year
          , EXTRACT(quarter FROM listings_src_26000.active_from) AS window_start__extract_quarter
          , EXTRACT(month FROM listings_src_26000.active_from) AS window_start__extract_month
          , EXTRACT(day FROM listings_src_26000.active_from) AS window_start__extract_day
          , EXTRACT(isodow FROM listings_src_26000.active_from) AS window_start__extract_dow
          , EXTRACT(doy FROM listings_src_26000.active_from) AS window_start__extract_doy
          , listings_src_26000.active_to AS window_end__day
          , DATE_TRUNC('week', listings_src_26000.active_to) AS window_end__week
          , DATE_TRUNC('month', listings_src_26000.active_to) AS window_end__month
          , DATE_TRUNC('quarter', listings_src_26000.active_to) AS window_end__quarter
          , DATE_TRUNC('year', listings_src_26000.active_to) AS window_end__year
          , EXTRACT(year FROM listings_src_26000.active_to) AS window_end__extract_year
          , EXTRACT(quarter FROM listings_src_26000.active_to) AS window_end__extract_quarter
          , EXTRACT(month FROM listings_src_26000.active_to) AS window_end__extract_month
          , EXTRACT(day FROM listings_src_26000.active_to) AS window_end__extract_day
          , EXTRACT(isodow FROM listings_src_26000.active_to) AS window_end__extract_dow
          , EXTRACT(doy FROM listings_src_26000.active_to) AS window_end__extract_doy
          , listings_src_26000.country
          , listings_src_26000.is_lux
          , listings_src_26000.capacity
          , listings_src_26000.active_from AS listing__window_start__day
          , DATE_TRUNC('week', listings_src_26000.active_from) AS listing__window_start__week
          , DATE_TRUNC('month', listings_src_26000.active_from) AS listing__window_start__month
          , DATE_TRUNC('quarter', listings_src_26000.active_from) AS listing__window_start__quarter
          , DATE_TRUNC('year', listings_src_26000.active_from) AS listing__window_start__year
          , EXTRACT(year FROM listings_src_26000.active_from) AS listing__window_start__extract_year
          , EXTRACT(quarter FROM listings_src_26000.active_from) AS listing__window_start__extract_quarter
          , EXTRACT(month FROM listings_src_26000.active_from) AS listing__window_start__extract_month
          , EXTRACT(day FROM listings_src_26000.active_from) AS listing__window_start__extract_day
          , EXTRACT(isodow FROM listings_src_26000.active_from) AS listing__window_start__extract_dow
          , EXTRACT(doy FROM listings_src_26000.active_from) AS listing__window_start__extract_doy
          , listings_src_26000.active_to AS listing__window_end__day
          , DATE_TRUNC('week', listings_src_26000.active_to) AS listing__window_end__week
          , DATE_TRUNC('month', listings_src_26000.active_to) AS listing__window_end__month
          , DATE_TRUNC('quarter', listings_src_26000.active_to) AS listing__window_end__quarter
          , DATE_TRUNC('year', listings_src_26000.active_to) AS listing__window_end__year
          , EXTRACT(year FROM listings_src_26000.active_to) AS listing__window_end__extract_year
          , EXTRACT(quarter FROM listings_src_26000.active_to) AS listing__window_end__extract_quarter
          , EXTRACT(month FROM listings_src_26000.active_to) AS listing__window_end__extract_month
          , EXTRACT(day FROM listings_src_26000.active_to) AS listing__window_end__extract_day
          , EXTRACT(isodow FROM listings_src_26000.active_to) AS listing__window_end__extract_dow
          , EXTRACT(doy FROM listings_src_26000.active_to) AS listing__window_end__extract_doy
          , listings_src_26000.country AS listing__country
          , listings_src_26000.is_lux AS listing__is_lux
          , listings_src_26000.capacity AS listing__capacity
          , listings_src_26000.listing_id AS listing
          , listings_src_26000.user_id AS user
          , listings_src_26000.user_id AS listing__user
        FROM ***************************.dim_listings listings_src_26000
      ) subq_2
      LEFT OUTER JOIN (
        -- Read From SemanticModelDataSet('users_latest')
        -- Pass Only Elements: [
        --   'home_state_latest',
        --   'user__home_state_latest',
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
        --   'user',
        -- ]
        SELECT
          DATE_TRUNC('day', users_latest_src_26000.ds) AS ds__day
          , DATE_TRUNC('week', users_latest_src_26000.ds) AS ds__week
          , DATE_TRUNC('month', users_latest_src_26000.ds) AS ds__month
          , DATE_TRUNC('quarter', users_latest_src_26000.ds) AS ds__quarter
          , DATE_TRUNC('year', users_latest_src_26000.ds) AS ds__year
          , EXTRACT(year FROM users_latest_src_26000.ds) AS ds__extract_year
          , EXTRACT(quarter FROM users_latest_src_26000.ds) AS ds__extract_quarter
          , EXTRACT(month FROM users_latest_src_26000.ds) AS ds__extract_month
          , EXTRACT(day FROM users_latest_src_26000.ds) AS ds__extract_day
          , EXTRACT(isodow FROM users_latest_src_26000.ds) AS ds__extract_dow
          , EXTRACT(doy FROM users_latest_src_26000.ds) AS ds__extract_doy
          , users_latest_src_26000.home_state_latest
          , DATE_TRUNC('day', users_latest_src_26000.ds) AS user__ds__day
          , DATE_TRUNC('week', users_latest_src_26000.ds) AS user__ds__week
          , DATE_TRUNC('month', users_latest_src_26000.ds) AS user__ds__month
          , DATE_TRUNC('quarter', users_latest_src_26000.ds) AS user__ds__quarter
          , DATE_TRUNC('year', users_latest_src_26000.ds) AS user__ds__year
          , EXTRACT(year FROM users_latest_src_26000.ds) AS user__ds__extract_year
          , EXTRACT(quarter FROM users_latest_src_26000.ds) AS user__ds__extract_quarter
          , EXTRACT(month FROM users_latest_src_26000.ds) AS user__ds__extract_month
          , EXTRACT(day FROM users_latest_src_26000.ds) AS user__ds__extract_day
          , EXTRACT(isodow FROM users_latest_src_26000.ds) AS user__ds__extract_dow
          , EXTRACT(doy FROM users_latest_src_26000.ds) AS user__ds__extract_doy
          , users_latest_src_26000.home_state_latest AS user__home_state_latest
          , users_latest_src_26000.user_id AS user
        FROM ***************************.dim_users_latest users_latest_src_26000
      ) subq_3
      ON
        subq_2.user = subq_3.user
    ) subq_4
    ON
      (
        subq_1.listing = subq_4.listing
      ) AND (
        (
          subq_1.metric_time__day >= subq_4.window_start__day
        ) AND (
          (
            subq_1.metric_time__day < subq_4.window_end__day
          ) OR (
            subq_4.window_end__day IS NULL
          )
        )
      )
  ) subq_5
  GROUP BY
    subq_5.metric_time__day
    , subq_5.listing__user__home_state_latest
) subq_6
