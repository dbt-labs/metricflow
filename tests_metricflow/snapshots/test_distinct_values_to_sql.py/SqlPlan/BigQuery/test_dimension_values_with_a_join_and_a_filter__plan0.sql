test_name: test_dimension_values_with_a_join_and_a_filter
test_filename: test_distinct_values_to_sql.py
docstring:
  Tests querying 2 dimensions that require a join and a filter.
sql_engine: BigQuery
---
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
SELECT
  nr_subq_4.listing__is_lux_latest
  , nr_subq_4.user__home_state_latest
FROM (
  -- Constrain Output with WHERE
  SELECT
    nr_subq_3.ds__day
    , nr_subq_3.ds__week
    , nr_subq_3.ds__month
    , nr_subq_3.ds__quarter
    , nr_subq_3.ds__year
    , nr_subq_3.ds__extract_year
    , nr_subq_3.ds__extract_quarter
    , nr_subq_3.ds__extract_month
    , nr_subq_3.ds__extract_day
    , nr_subq_3.ds__extract_dow
    , nr_subq_3.ds__extract_doy
    , nr_subq_3.created_at__day
    , nr_subq_3.created_at__week
    , nr_subq_3.created_at__month
    , nr_subq_3.created_at__quarter
    , nr_subq_3.created_at__year
    , nr_subq_3.created_at__extract_year
    , nr_subq_3.created_at__extract_quarter
    , nr_subq_3.created_at__extract_month
    , nr_subq_3.created_at__extract_day
    , nr_subq_3.created_at__extract_dow
    , nr_subq_3.created_at__extract_doy
    , nr_subq_3.listing__ds__day
    , nr_subq_3.listing__ds__week
    , nr_subq_3.listing__ds__month
    , nr_subq_3.listing__ds__quarter
    , nr_subq_3.listing__ds__year
    , nr_subq_3.listing__ds__extract_year
    , nr_subq_3.listing__ds__extract_quarter
    , nr_subq_3.listing__ds__extract_month
    , nr_subq_3.listing__ds__extract_day
    , nr_subq_3.listing__ds__extract_dow
    , nr_subq_3.listing__ds__extract_doy
    , nr_subq_3.listing__created_at__day
    , nr_subq_3.listing__created_at__week
    , nr_subq_3.listing__created_at__month
    , nr_subq_3.listing__created_at__quarter
    , nr_subq_3.listing__created_at__year
    , nr_subq_3.listing__created_at__extract_year
    , nr_subq_3.listing__created_at__extract_quarter
    , nr_subq_3.listing__created_at__extract_month
    , nr_subq_3.listing__created_at__extract_day
    , nr_subq_3.listing__created_at__extract_dow
    , nr_subq_3.listing__created_at__extract_doy
    , nr_subq_3.listing
    , nr_subq_3.user
    , nr_subq_3.listing__user
    , nr_subq_3.country_latest
    , nr_subq_3.is_lux_latest
    , nr_subq_3.capacity_latest
    , nr_subq_3.listing__country_latest
    , nr_subq_3.listing__is_lux_latest
    , nr_subq_3.listing__capacity_latest
    , nr_subq_3.user__home_state_latest
    , nr_subq_3.listings
    , nr_subq_3.largest_listing
    , nr_subq_3.smallest_listing
  FROM (
    -- Join Standard Outputs
    SELECT
      nr_subq_2.home_state_latest AS user__home_state_latest
      , nr_subq_0.ds__day AS ds__day
      , nr_subq_0.ds__week AS ds__week
      , nr_subq_0.ds__month AS ds__month
      , nr_subq_0.ds__quarter AS ds__quarter
      , nr_subq_0.ds__year AS ds__year
      , nr_subq_0.ds__extract_year AS ds__extract_year
      , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
      , nr_subq_0.ds__extract_month AS ds__extract_month
      , nr_subq_0.ds__extract_day AS ds__extract_day
      , nr_subq_0.ds__extract_dow AS ds__extract_dow
      , nr_subq_0.ds__extract_doy AS ds__extract_doy
      , nr_subq_0.created_at__day AS created_at__day
      , nr_subq_0.created_at__week AS created_at__week
      , nr_subq_0.created_at__month AS created_at__month
      , nr_subq_0.created_at__quarter AS created_at__quarter
      , nr_subq_0.created_at__year AS created_at__year
      , nr_subq_0.created_at__extract_year AS created_at__extract_year
      , nr_subq_0.created_at__extract_quarter AS created_at__extract_quarter
      , nr_subq_0.created_at__extract_month AS created_at__extract_month
      , nr_subq_0.created_at__extract_day AS created_at__extract_day
      , nr_subq_0.created_at__extract_dow AS created_at__extract_dow
      , nr_subq_0.created_at__extract_doy AS created_at__extract_doy
      , nr_subq_0.listing__ds__day AS listing__ds__day
      , nr_subq_0.listing__ds__week AS listing__ds__week
      , nr_subq_0.listing__ds__month AS listing__ds__month
      , nr_subq_0.listing__ds__quarter AS listing__ds__quarter
      , nr_subq_0.listing__ds__year AS listing__ds__year
      , nr_subq_0.listing__ds__extract_year AS listing__ds__extract_year
      , nr_subq_0.listing__ds__extract_quarter AS listing__ds__extract_quarter
      , nr_subq_0.listing__ds__extract_month AS listing__ds__extract_month
      , nr_subq_0.listing__ds__extract_day AS listing__ds__extract_day
      , nr_subq_0.listing__ds__extract_dow AS listing__ds__extract_dow
      , nr_subq_0.listing__ds__extract_doy AS listing__ds__extract_doy
      , nr_subq_0.listing__created_at__day AS listing__created_at__day
      , nr_subq_0.listing__created_at__week AS listing__created_at__week
      , nr_subq_0.listing__created_at__month AS listing__created_at__month
      , nr_subq_0.listing__created_at__quarter AS listing__created_at__quarter
      , nr_subq_0.listing__created_at__year AS listing__created_at__year
      , nr_subq_0.listing__created_at__extract_year AS listing__created_at__extract_year
      , nr_subq_0.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
      , nr_subq_0.listing__created_at__extract_month AS listing__created_at__extract_month
      , nr_subq_0.listing__created_at__extract_day AS listing__created_at__extract_day
      , nr_subq_0.listing__created_at__extract_dow AS listing__created_at__extract_dow
      , nr_subq_0.listing__created_at__extract_doy AS listing__created_at__extract_doy
      , nr_subq_0.listing AS listing
      , nr_subq_0.user AS user
      , nr_subq_0.listing__user AS listing__user
      , nr_subq_0.country_latest AS country_latest
      , nr_subq_0.is_lux_latest AS is_lux_latest
      , nr_subq_0.capacity_latest AS capacity_latest
      , nr_subq_0.listing__country_latest AS listing__country_latest
      , nr_subq_0.listing__is_lux_latest AS listing__is_lux_latest
      , nr_subq_0.listing__capacity_latest AS listing__capacity_latest
      , nr_subq_0.listings AS listings
      , nr_subq_0.largest_listing AS largest_listing
      , nr_subq_0.smallest_listing AS smallest_listing
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      SELECT
        1 AS listings
        , listings_latest_src_28000.capacity AS largest_listing
        , listings_latest_src_28000.capacity AS smallest_listing
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
        , listings_latest_src_28000.country AS country_latest
        , listings_latest_src_28000.is_lux AS is_lux_latest
        , listings_latest_src_28000.capacity AS capacity_latest
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
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
    ) nr_subq_0
    FULL OUTER JOIN (
      -- Pass Only Elements: ['home_state_latest', 'user']
      SELECT
        nr_subq_1.user
        , nr_subq_1.home_state_latest
      FROM (
        -- Read Elements From Semantic Model 'users_latest'
        SELECT
          DATETIME_TRUNC(users_latest_src_28000.ds, day) AS ds_latest__day
          , DATETIME_TRUNC(users_latest_src_28000.ds, isoweek) AS ds_latest__week
          , DATETIME_TRUNC(users_latest_src_28000.ds, month) AS ds_latest__month
          , DATETIME_TRUNC(users_latest_src_28000.ds, quarter) AS ds_latest__quarter
          , DATETIME_TRUNC(users_latest_src_28000.ds, year) AS ds_latest__year
          , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
          , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
          , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
          , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
          , IF(EXTRACT(dayofweek FROM users_latest_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_latest_src_28000.ds) - 1) AS ds_latest__extract_dow
          , EXTRACT(dayofyear FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
          , users_latest_src_28000.home_state_latest
          , DATETIME_TRUNC(users_latest_src_28000.ds, day) AS user__ds_latest__day
          , DATETIME_TRUNC(users_latest_src_28000.ds, isoweek) AS user__ds_latest__week
          , DATETIME_TRUNC(users_latest_src_28000.ds, month) AS user__ds_latest__month
          , DATETIME_TRUNC(users_latest_src_28000.ds, quarter) AS user__ds_latest__quarter
          , DATETIME_TRUNC(users_latest_src_28000.ds, year) AS user__ds_latest__year
          , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
          , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
          , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
          , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
          , IF(EXTRACT(dayofweek FROM users_latest_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_latest_src_28000.ds) - 1) AS user__ds_latest__extract_dow
          , EXTRACT(dayofyear FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
          , users_latest_src_28000.home_state_latest AS user__home_state_latest
          , users_latest_src_28000.user_id AS user
        FROM ***************************.dim_users_latest users_latest_src_28000
      ) nr_subq_1
    ) nr_subq_2
    ON
      nr_subq_0.user = nr_subq_2.user
  ) nr_subq_3
  WHERE user__home_state_latest = 'us'
) nr_subq_4
GROUP BY
  listing__is_lux_latest
  , user__home_state_latest
