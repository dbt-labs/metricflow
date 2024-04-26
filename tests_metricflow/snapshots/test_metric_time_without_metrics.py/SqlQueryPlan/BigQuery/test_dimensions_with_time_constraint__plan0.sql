-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
SELECT
  subq_9.metric_time__day
  , subq_9.listing__is_lux_latest
  , subq_9.user__home_state_latest
FROM (
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-03T00:00:00]
  SELECT
    subq_8.ds__day
    , subq_8.ds__week
    , subq_8.ds__month
    , subq_8.ds__quarter
    , subq_8.ds__year
    , subq_8.ds__extract_year
    , subq_8.ds__extract_quarter
    , subq_8.ds__extract_month
    , subq_8.ds__extract_day
    , subq_8.ds__extract_dow
    , subq_8.ds__extract_doy
    , subq_8.created_at__day
    , subq_8.created_at__week
    , subq_8.created_at__month
    , subq_8.created_at__quarter
    , subq_8.created_at__year
    , subq_8.created_at__extract_year
    , subq_8.created_at__extract_quarter
    , subq_8.created_at__extract_month
    , subq_8.created_at__extract_day
    , subq_8.created_at__extract_dow
    , subq_8.created_at__extract_doy
    , subq_8.listing__ds__day
    , subq_8.listing__ds__week
    , subq_8.listing__ds__month
    , subq_8.listing__ds__quarter
    , subq_8.listing__ds__year
    , subq_8.listing__ds__extract_year
    , subq_8.listing__ds__extract_quarter
    , subq_8.listing__ds__extract_month
    , subq_8.listing__ds__extract_day
    , subq_8.listing__ds__extract_dow
    , subq_8.listing__ds__extract_doy
    , subq_8.listing__created_at__day
    , subq_8.listing__created_at__week
    , subq_8.listing__created_at__month
    , subq_8.listing__created_at__quarter
    , subq_8.listing__created_at__year
    , subq_8.listing__created_at__extract_year
    , subq_8.listing__created_at__extract_quarter
    , subq_8.listing__created_at__extract_month
    , subq_8.listing__created_at__extract_day
    , subq_8.listing__created_at__extract_dow
    , subq_8.listing__created_at__extract_doy
    , subq_8.metric_time__day
    , subq_8.listing
    , subq_8.user
    , subq_8.listing__user
    , subq_8.country_latest
    , subq_8.is_lux_latest
    , subq_8.capacity_latest
    , subq_8.listing__country_latest
    , subq_8.listing__is_lux_latest
    , subq_8.listing__capacity_latest
    , subq_8.user__home_state_latest
    , subq_8.listings
    , subq_8.largest_listing
    , subq_8.smallest_listing
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_2.ds__day AS ds__day
      , subq_2.ds__week AS ds__week
      , subq_2.ds__month AS ds__month
      , subq_2.ds__quarter AS ds__quarter
      , subq_2.ds__year AS ds__year
      , subq_2.ds__extract_year AS ds__extract_year
      , subq_2.ds__extract_quarter AS ds__extract_quarter
      , subq_2.ds__extract_month AS ds__extract_month
      , subq_2.ds__extract_day AS ds__extract_day
      , subq_2.ds__extract_dow AS ds__extract_dow
      , subq_2.ds__extract_doy AS ds__extract_doy
      , subq_2.created_at__day AS created_at__day
      , subq_2.created_at__week AS created_at__week
      , subq_2.created_at__month AS created_at__month
      , subq_2.created_at__quarter AS created_at__quarter
      , subq_2.created_at__year AS created_at__year
      , subq_2.created_at__extract_year AS created_at__extract_year
      , subq_2.created_at__extract_quarter AS created_at__extract_quarter
      , subq_2.created_at__extract_month AS created_at__extract_month
      , subq_2.created_at__extract_day AS created_at__extract_day
      , subq_2.created_at__extract_dow AS created_at__extract_dow
      , subq_2.created_at__extract_doy AS created_at__extract_doy
      , subq_2.listing__ds__day AS listing__ds__day
      , subq_2.listing__ds__week AS listing__ds__week
      , subq_2.listing__ds__month AS listing__ds__month
      , subq_2.listing__ds__quarter AS listing__ds__quarter
      , subq_2.listing__ds__year AS listing__ds__year
      , subq_2.listing__ds__extract_year AS listing__ds__extract_year
      , subq_2.listing__ds__extract_quarter AS listing__ds__extract_quarter
      , subq_2.listing__ds__extract_month AS listing__ds__extract_month
      , subq_2.listing__ds__extract_day AS listing__ds__extract_day
      , subq_2.listing__ds__extract_dow AS listing__ds__extract_dow
      , subq_2.listing__ds__extract_doy AS listing__ds__extract_doy
      , subq_2.listing__created_at__day AS listing__created_at__day
      , subq_2.listing__created_at__week AS listing__created_at__week
      , subq_2.listing__created_at__month AS listing__created_at__month
      , subq_2.listing__created_at__quarter AS listing__created_at__quarter
      , subq_2.listing__created_at__year AS listing__created_at__year
      , subq_2.listing__created_at__extract_year AS listing__created_at__extract_year
      , subq_2.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
      , subq_2.listing__created_at__extract_month AS listing__created_at__extract_month
      , subq_2.listing__created_at__extract_day AS listing__created_at__extract_day
      , subq_2.listing__created_at__extract_dow AS listing__created_at__extract_dow
      , subq_2.listing__created_at__extract_doy AS listing__created_at__extract_doy
      , subq_5.metric_time__day AS metric_time__day
      , subq_2.listing AS listing
      , subq_2.user AS user
      , subq_2.listing__user AS listing__user
      , subq_2.country_latest AS country_latest
      , subq_2.is_lux_latest AS is_lux_latest
      , subq_2.capacity_latest AS capacity_latest
      , subq_2.listing__country_latest AS listing__country_latest
      , subq_2.listing__is_lux_latest AS listing__is_lux_latest
      , subq_2.listing__capacity_latest AS listing__capacity_latest
      , subq_7.home_state_latest AS user__home_state_latest
      , subq_2.listings AS listings
      , subq_2.largest_listing AS largest_listing
      , subq_2.smallest_listing AS smallest_listing
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      SELECT
        1 AS listings
        , listings_latest_src_28000.capacity AS largest_listing
        , listings_latest_src_28000.capacity AS smallest_listing
        , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
        , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
        , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
        , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
        , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
        , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
        , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
        , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
        , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
        , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
        , listings_latest_src_28000.country AS country_latest
        , listings_latest_src_28000.is_lux AS is_lux_latest
        , listings_latest_src_28000.capacity AS capacity_latest
        , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
        , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
        , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
        , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
        , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
        , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
        , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
        , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
        , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
        , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__created_at__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
        , listings_latest_src_28000.country AS listing__country_latest
        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , listings_latest_src_28000.capacity AS listing__capacity_latest
        , listings_latest_src_28000.listing_id AS listing
        , listings_latest_src_28000.user_id AS user
        , listings_latest_src_28000.user_id AS listing__user
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_2
    CROSS JOIN (
      -- Pass Only Elements: ['metric_time__day',]
      SELECT
        subq_4.metric_time__day
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
        FROM (
          -- Time Spine
          SELECT
            DATE_TRUNC(time_spine_src_28000.ds, day) AS ds__day
            , DATE_TRUNC(time_spine_src_28000.ds, isoweek) AS ds__week
            , DATE_TRUNC(time_spine_src_28000.ds, month) AS ds__month
            , DATE_TRUNC(time_spine_src_28000.ds, quarter) AS ds__quarter
            , DATE_TRUNC(time_spine_src_28000.ds, year) AS ds__year
            , EXTRACT(year FROM time_spine_src_28000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM time_spine_src_28000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM time_spine_src_28000.ds) AS ds__extract_month
            , EXTRACT(day FROM time_spine_src_28000.ds) AS ds__extract_day
            , IF(EXTRACT(dayofweek FROM time_spine_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28000.ds) - 1) AS ds__extract_dow
            , EXTRACT(dayofyear FROM time_spine_src_28000.ds) AS ds__extract_doy
          FROM ***************************.mf_time_spine time_spine_src_28000
        ) subq_3
      ) subq_4
    ) subq_5
    FULL OUTER JOIN (
      -- Pass Only Elements: ['home_state_latest', 'user']
      SELECT
        subq_6.user
        , subq_6.home_state_latest
      FROM (
        -- Read Elements From Semantic Model 'users_latest'
        SELECT
          DATE_TRUNC(users_latest_src_28000.ds, day) AS ds_latest__day
          , DATE_TRUNC(users_latest_src_28000.ds, isoweek) AS ds_latest__week
          , DATE_TRUNC(users_latest_src_28000.ds, month) AS ds_latest__month
          , DATE_TRUNC(users_latest_src_28000.ds, quarter) AS ds_latest__quarter
          , DATE_TRUNC(users_latest_src_28000.ds, year) AS ds_latest__year
          , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
          , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
          , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
          , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
          , IF(EXTRACT(dayofweek FROM users_latest_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_latest_src_28000.ds) - 1) AS ds_latest__extract_dow
          , EXTRACT(dayofyear FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
          , users_latest_src_28000.home_state_latest
          , DATE_TRUNC(users_latest_src_28000.ds, day) AS user__ds_latest__day
          , DATE_TRUNC(users_latest_src_28000.ds, isoweek) AS user__ds_latest__week
          , DATE_TRUNC(users_latest_src_28000.ds, month) AS user__ds_latest__month
          , DATE_TRUNC(users_latest_src_28000.ds, quarter) AS user__ds_latest__quarter
          , DATE_TRUNC(users_latest_src_28000.ds, year) AS user__ds_latest__year
          , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
          , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
          , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
          , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
          , IF(EXTRACT(dayofweek FROM users_latest_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_latest_src_28000.ds) - 1) AS user__ds_latest__extract_dow
          , EXTRACT(dayofyear FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
          , users_latest_src_28000.home_state_latest AS user__home_state_latest
          , users_latest_src_28000.user_id AS user
        FROM ***************************.dim_users_latest users_latest_src_28000
      ) subq_6
    ) subq_7
    ON
      subq_2.user = subq_7.user
  ) subq_8
  WHERE subq_8.metric_time__day BETWEEN '2020-01-01' AND '2020-01-03'
) subq_9
GROUP BY
  metric_time__day
  , listing__is_lux_latest
  , user__home_state_latest
