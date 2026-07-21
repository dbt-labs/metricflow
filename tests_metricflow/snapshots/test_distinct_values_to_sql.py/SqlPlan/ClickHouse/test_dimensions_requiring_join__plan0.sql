test_name: test_dimensions_requiring_join
test_filename: test_distinct_values_to_sql.py
docstring:
  Tests querying 2 dimensions that require a join.
sql_engine: ClickHouse
---
SELECT
  subq_5.listing__is_lux_latest
  , subq_5.user__home_state_latest
FROM (
  SELECT
    subq_4.listing__is_lux_latest
    , subq_4.user__home_state_latest
  FROM (
    SELECT
      subq_3.listing__is_lux_latest
      , subq_3.user__home_state_latest
    FROM (
      SELECT
        subq_2.home_state_latest AS user__home_state_latest
        , subq_0.ds__day AS ds__day
        , subq_0.ds__week AS ds__week
        , subq_0.ds__month AS ds__month
        , subq_0.ds__quarter AS ds__quarter
        , subq_0.ds__year AS ds__year
        , subq_0.ds__extract_year AS ds__extract_year
        , subq_0.ds__extract_quarter AS ds__extract_quarter
        , subq_0.ds__extract_month AS ds__extract_month
        , subq_0.ds__extract_day AS ds__extract_day
        , subq_0.ds__extract_dow AS ds__extract_dow
        , subq_0.ds__extract_doy AS ds__extract_doy
        , subq_0.created_at__day AS created_at__day
        , subq_0.created_at__week AS created_at__week
        , subq_0.created_at__month AS created_at__month
        , subq_0.created_at__quarter AS created_at__quarter
        , subq_0.created_at__year AS created_at__year
        , subq_0.created_at__extract_year AS created_at__extract_year
        , subq_0.created_at__extract_quarter AS created_at__extract_quarter
        , subq_0.created_at__extract_month AS created_at__extract_month
        , subq_0.created_at__extract_day AS created_at__extract_day
        , subq_0.created_at__extract_dow AS created_at__extract_dow
        , subq_0.created_at__extract_doy AS created_at__extract_doy
        , subq_0.listing__ds__day AS listing__ds__day
        , subq_0.listing__ds__week AS listing__ds__week
        , subq_0.listing__ds__month AS listing__ds__month
        , subq_0.listing__ds__quarter AS listing__ds__quarter
        , subq_0.listing__ds__year AS listing__ds__year
        , subq_0.listing__ds__extract_year AS listing__ds__extract_year
        , subq_0.listing__ds__extract_quarter AS listing__ds__extract_quarter
        , subq_0.listing__ds__extract_month AS listing__ds__extract_month
        , subq_0.listing__ds__extract_day AS listing__ds__extract_day
        , subq_0.listing__ds__extract_dow AS listing__ds__extract_dow
        , subq_0.listing__ds__extract_doy AS listing__ds__extract_doy
        , subq_0.listing__created_at__day AS listing__created_at__day
        , subq_0.listing__created_at__week AS listing__created_at__week
        , subq_0.listing__created_at__month AS listing__created_at__month
        , subq_0.listing__created_at__quarter AS listing__created_at__quarter
        , subq_0.listing__created_at__year AS listing__created_at__year
        , subq_0.listing__created_at__extract_year AS listing__created_at__extract_year
        , subq_0.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
        , subq_0.listing__created_at__extract_month AS listing__created_at__extract_month
        , subq_0.listing__created_at__extract_day AS listing__created_at__extract_day
        , subq_0.listing__created_at__extract_dow AS listing__created_at__extract_dow
        , subq_0.listing__created_at__extract_doy AS listing__created_at__extract_doy
        , subq_0.listing AS listing
        , subq_0.user AS user
        , subq_0.listing__user AS listing__user
        , subq_0.country_latest AS country_latest
        , subq_0.is_lux_latest AS is_lux_latest
        , subq_0.capacity_latest AS capacity_latest
        , subq_0.listing__country_latest AS listing__country_latest
        , subq_0.listing__is_lux_latest AS listing__is_lux_latest
        , subq_0.listing__capacity_latest AS listing__capacity_latest
        , subq_0.__listings AS __listings
        , subq_0.__lux_listings AS __lux_listings
        , subq_0.__smallest_listing AS __smallest_listing
        , subq_0.__largest_listing AS __largest_listing
        , subq_0.__active_listings AS __active_listings
      FROM (
        SELECT
          1 AS __listings
          , 1 AS __lux_listings
          , listings_latest_src_28000.capacity AS __smallest_listing
          , listings_latest_src_28000.capacity AS __largest_listing
          , 1 AS __active_listings
          , toStartOfDay(listings_latest_src_28000.created_at) AS ds__day
          , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS ds__week
          , toStartOfMonth(listings_latest_src_28000.created_at) AS ds__month
          , toStartOfQuarter(listings_latest_src_28000.created_at) AS ds__quarter
          , toStartOfYear(listings_latest_src_28000.created_at) AS ds__year
          , toYear(listings_latest_src_28000.created_at) AS ds__extract_year
          , toQuarter(listings_latest_src_28000.created_at) AS ds__extract_quarter
          , toMonth(listings_latest_src_28000.created_at) AS ds__extract_month
          , toDayOfMonth(listings_latest_src_28000.created_at) AS ds__extract_day
          , toDayOfWeek(listings_latest_src_28000.created_at) AS ds__extract_dow
          , toDayOfYear(listings_latest_src_28000.created_at) AS ds__extract_doy
          , toStartOfDay(listings_latest_src_28000.created_at) AS created_at__day
          , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS created_at__week
          , toStartOfMonth(listings_latest_src_28000.created_at) AS created_at__month
          , toStartOfQuarter(listings_latest_src_28000.created_at) AS created_at__quarter
          , toStartOfYear(listings_latest_src_28000.created_at) AS created_at__year
          , toYear(listings_latest_src_28000.created_at) AS created_at__extract_year
          , toQuarter(listings_latest_src_28000.created_at) AS created_at__extract_quarter
          , toMonth(listings_latest_src_28000.created_at) AS created_at__extract_month
          , toDayOfMonth(listings_latest_src_28000.created_at) AS created_at__extract_day
          , toDayOfWeek(listings_latest_src_28000.created_at) AS created_at__extract_dow
          , toDayOfYear(listings_latest_src_28000.created_at) AS created_at__extract_doy
          , listings_latest_src_28000.country AS country_latest
          , listings_latest_src_28000.is_lux AS is_lux_latest
          , listings_latest_src_28000.capacity AS capacity_latest
          , toStartOfDay(listings_latest_src_28000.created_at) AS listing__ds__day
          , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS listing__ds__week
          , toStartOfMonth(listings_latest_src_28000.created_at) AS listing__ds__month
          , toStartOfQuarter(listings_latest_src_28000.created_at) AS listing__ds__quarter
          , toStartOfYear(listings_latest_src_28000.created_at) AS listing__ds__year
          , toYear(listings_latest_src_28000.created_at) AS listing__ds__extract_year
          , toQuarter(listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
          , toMonth(listings_latest_src_28000.created_at) AS listing__ds__extract_month
          , toDayOfMonth(listings_latest_src_28000.created_at) AS listing__ds__extract_day
          , toDayOfWeek(listings_latest_src_28000.created_at) AS listing__ds__extract_dow
          , toDayOfYear(listings_latest_src_28000.created_at) AS listing__ds__extract_doy
          , toStartOfDay(listings_latest_src_28000.created_at) AS listing__created_at__day
          , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS listing__created_at__week
          , toStartOfMonth(listings_latest_src_28000.created_at) AS listing__created_at__month
          , toStartOfQuarter(listings_latest_src_28000.created_at) AS listing__created_at__quarter
          , toStartOfYear(listings_latest_src_28000.created_at) AS listing__created_at__year
          , toYear(listings_latest_src_28000.created_at) AS listing__created_at__extract_year
          , toQuarter(listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
          , toMonth(listings_latest_src_28000.created_at) AS listing__created_at__extract_month
          , toDayOfMonth(listings_latest_src_28000.created_at) AS listing__created_at__extract_day
          , toDayOfWeek(listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
          , toDayOfYear(listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
          , listings_latest_src_28000.country AS listing__country_latest
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , listings_latest_src_28000.capacity AS listing__capacity_latest
          , listings_latest_src_28000.listing_id AS listing
          , listings_latest_src_28000.user_id AS user
          , listings_latest_src_28000.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_28000
      ) subq_0
      FULL OUTER JOIN (
        SELECT
          subq_1.user
          , subq_1.home_state_latest
        FROM (
          SELECT
            toStartOfDay(users_latest_src_28000.ds) AS ds_latest__day
            , toStartOfWeek(users_latest_src_28000.ds, 1) AS ds_latest__week
            , toStartOfMonth(users_latest_src_28000.ds) AS ds_latest__month
            , toStartOfQuarter(users_latest_src_28000.ds) AS ds_latest__quarter
            , toStartOfYear(users_latest_src_28000.ds) AS ds_latest__year
            , toYear(users_latest_src_28000.ds) AS ds_latest__extract_year
            , toQuarter(users_latest_src_28000.ds) AS ds_latest__extract_quarter
            , toMonth(users_latest_src_28000.ds) AS ds_latest__extract_month
            , toDayOfMonth(users_latest_src_28000.ds) AS ds_latest__extract_day
            , toDayOfWeek(users_latest_src_28000.ds) AS ds_latest__extract_dow
            , toDayOfYear(users_latest_src_28000.ds) AS ds_latest__extract_doy
            , users_latest_src_28000.home_state_latest
            , toStartOfDay(users_latest_src_28000.ds) AS user__ds_latest__day
            , toStartOfWeek(users_latest_src_28000.ds, 1) AS user__ds_latest__week
            , toStartOfMonth(users_latest_src_28000.ds) AS user__ds_latest__month
            , toStartOfQuarter(users_latest_src_28000.ds) AS user__ds_latest__quarter
            , toStartOfYear(users_latest_src_28000.ds) AS user__ds_latest__year
            , toYear(users_latest_src_28000.ds) AS user__ds_latest__extract_year
            , toQuarter(users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
            , toMonth(users_latest_src_28000.ds) AS user__ds_latest__extract_month
            , toDayOfMonth(users_latest_src_28000.ds) AS user__ds_latest__extract_day
            , toDayOfWeek(users_latest_src_28000.ds) AS user__ds_latest__extract_dow
            , toDayOfYear(users_latest_src_28000.ds) AS user__ds_latest__extract_doy
            , users_latest_src_28000.home_state_latest AS user__home_state_latest
            , users_latest_src_28000.user_id AS user
          FROM ***************************.dim_users_latest users_latest_src_28000
        ) subq_1
      ) subq_2
      ON
        subq_0.user = subq_2.user
    ) subq_3
  ) subq_4
  GROUP BY
    subq_4.listing__is_lux_latest
    , subq_4.user__home_state_latest
) subq_5
