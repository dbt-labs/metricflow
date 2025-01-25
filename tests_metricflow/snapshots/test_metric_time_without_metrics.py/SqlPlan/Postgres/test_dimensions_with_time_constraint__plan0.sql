test_name: test_dimensions_with_time_constraint
test_filename: test_metric_time_without_metrics.py
sql_engine: Postgres
---
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
SELECT
  nr_subq_6.metric_time__day
  , nr_subq_6.listing__is_lux_latest
  , nr_subq_6.user__home_state_latest
FROM (
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-03T00:00:00]
  SELECT
    nr_subq_5.ds__day
    , nr_subq_5.ds__week
    , nr_subq_5.ds__month
    , nr_subq_5.ds__quarter
    , nr_subq_5.ds__year
    , nr_subq_5.ds__extract_year
    , nr_subq_5.ds__extract_quarter
    , nr_subq_5.ds__extract_month
    , nr_subq_5.ds__extract_day
    , nr_subq_5.ds__extract_dow
    , nr_subq_5.ds__extract_doy
    , nr_subq_5.created_at__day
    , nr_subq_5.created_at__week
    , nr_subq_5.created_at__month
    , nr_subq_5.created_at__quarter
    , nr_subq_5.created_at__year
    , nr_subq_5.created_at__extract_year
    , nr_subq_5.created_at__extract_quarter
    , nr_subq_5.created_at__extract_month
    , nr_subq_5.created_at__extract_day
    , nr_subq_5.created_at__extract_dow
    , nr_subq_5.created_at__extract_doy
    , nr_subq_5.listing__ds__day
    , nr_subq_5.listing__ds__week
    , nr_subq_5.listing__ds__month
    , nr_subq_5.listing__ds__quarter
    , nr_subq_5.listing__ds__year
    , nr_subq_5.listing__ds__extract_year
    , nr_subq_5.listing__ds__extract_quarter
    , nr_subq_5.listing__ds__extract_month
    , nr_subq_5.listing__ds__extract_day
    , nr_subq_5.listing__ds__extract_dow
    , nr_subq_5.listing__ds__extract_doy
    , nr_subq_5.listing__created_at__day
    , nr_subq_5.listing__created_at__week
    , nr_subq_5.listing__created_at__month
    , nr_subq_5.listing__created_at__quarter
    , nr_subq_5.listing__created_at__year
    , nr_subq_5.listing__created_at__extract_year
    , nr_subq_5.listing__created_at__extract_quarter
    , nr_subq_5.listing__created_at__extract_month
    , nr_subq_5.listing__created_at__extract_day
    , nr_subq_5.listing__created_at__extract_dow
    , nr_subq_5.listing__created_at__extract_doy
    , nr_subq_5.metric_time__day
    , nr_subq_5.listing
    , nr_subq_5.user
    , nr_subq_5.listing__user
    , nr_subq_5.country_latest
    , nr_subq_5.is_lux_latest
    , nr_subq_5.capacity_latest
    , nr_subq_5.listing__country_latest
    , nr_subq_5.listing__is_lux_latest
    , nr_subq_5.listing__capacity_latest
    , nr_subq_5.user__home_state_latest
    , nr_subq_5.listings
    , nr_subq_5.largest_listing
    , nr_subq_5.smallest_listing
  FROM (
    -- Join Standard Outputs
    SELECT
      nr_subq_4.home_state_latest AS user__home_state_latest
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
      , nr_subq_2.metric_time__day AS metric_time__day
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
    ) nr_subq_0
    CROSS JOIN (
      -- Pass Only Elements: ['metric_time__day',]
      SELECT
        nr_subq_1.metric_time__day
      FROM (
        -- Metric Time Dimension 'ds'
        SELECT
          nr_subq_28019.ds__day
          , nr_subq_28019.ds__week
          , nr_subq_28019.ds__month
          , nr_subq_28019.ds__quarter
          , nr_subq_28019.ds__year
          , nr_subq_28019.ds__extract_year
          , nr_subq_28019.ds__extract_quarter
          , nr_subq_28019.ds__extract_month
          , nr_subq_28019.ds__extract_day
          , nr_subq_28019.ds__extract_dow
          , nr_subq_28019.ds__extract_doy
          , nr_subq_28019.ds__martian_day
          , nr_subq_28019.ds__day AS metric_time__day
          , nr_subq_28019.ds__week AS metric_time__week
          , nr_subq_28019.ds__month AS metric_time__month
          , nr_subq_28019.ds__quarter AS metric_time__quarter
          , nr_subq_28019.ds__year AS metric_time__year
          , nr_subq_28019.ds__extract_year AS metric_time__extract_year
          , nr_subq_28019.ds__extract_quarter AS metric_time__extract_quarter
          , nr_subq_28019.ds__extract_month AS metric_time__extract_month
          , nr_subq_28019.ds__extract_day AS metric_time__extract_day
          , nr_subq_28019.ds__extract_dow AS metric_time__extract_dow
          , nr_subq_28019.ds__extract_doy AS metric_time__extract_doy
          , nr_subq_28019.ds__martian_day AS metric_time__martian_day
        FROM (
          -- Read From Time Spine 'mf_time_spine'
          SELECT
            time_spine_src_28006.ds AS ds__day
            , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
            , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
            , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
            , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
            , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
            , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
            , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
            , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
            , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
            , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
            , time_spine_src_28006.martian_day AS ds__martian_day
          FROM ***************************.mf_time_spine time_spine_src_28006
        ) nr_subq_28019
      ) nr_subq_1
    ) nr_subq_2
    FULL OUTER JOIN (
      -- Pass Only Elements: ['home_state_latest', 'user']
      SELECT
        nr_subq_3.user
        , nr_subq_3.home_state_latest
      FROM (
        -- Read Elements From Semantic Model 'users_latest'
        SELECT
          DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
          , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
          , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
          , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
          , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
          , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
          , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
          , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
          , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
          , EXTRACT(isodow FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
          , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
          , users_latest_src_28000.home_state_latest
          , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
          , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
          , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
          , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
          , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
          , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
          , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
          , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
          , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
          , EXTRACT(isodow FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
          , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
          , users_latest_src_28000.home_state_latest AS user__home_state_latest
          , users_latest_src_28000.user_id AS user
        FROM ***************************.dim_users_latest users_latest_src_28000
      ) nr_subq_3
    ) nr_subq_4
    ON
      nr_subq_0.user = nr_subq_4.user
  ) nr_subq_5
  WHERE nr_subq_5.metric_time__day BETWEEN '2020-01-01' AND '2020-01-03'
) nr_subq_6
GROUP BY
  nr_subq_6.metric_time__day
  , nr_subq_6.listing__is_lux_latest
  , nr_subq_6.user__home_state_latest
