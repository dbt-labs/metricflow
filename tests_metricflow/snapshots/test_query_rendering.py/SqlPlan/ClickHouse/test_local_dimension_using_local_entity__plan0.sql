test_name: test_local_dimension_using_local_entity
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_5.listing__country_latest
  , subq_5.listings
FROM (
  SELECT
    subq_4.listing__country_latest
    , subq_4.__listings AS listings
  FROM (
    SELECT
      subq_3.listing__country_latest
      , SUM(subq_3.__listings) AS __listings
    FROM (
      SELECT
        subq_2.listing__country_latest
        , subq_2.__listings
      FROM (
        SELECT
          subq_1.listing__country_latest
          , subq_1.__listings
        FROM (
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
            , subq_0.created_at__day
            , subq_0.created_at__week
            , subq_0.created_at__month
            , subq_0.created_at__quarter
            , subq_0.created_at__year
            , subq_0.created_at__extract_year
            , subq_0.created_at__extract_quarter
            , subq_0.created_at__extract_month
            , subq_0.created_at__extract_day
            , subq_0.created_at__extract_dow
            , subq_0.created_at__extract_doy
            , subq_0.listing__ds__day
            , subq_0.listing__ds__week
            , subq_0.listing__ds__month
            , subq_0.listing__ds__quarter
            , subq_0.listing__ds__year
            , subq_0.listing__ds__extract_year
            , subq_0.listing__ds__extract_quarter
            , subq_0.listing__ds__extract_month
            , subq_0.listing__ds__extract_day
            , subq_0.listing__ds__extract_dow
            , subq_0.listing__ds__extract_doy
            , subq_0.listing__created_at__day
            , subq_0.listing__created_at__week
            , subq_0.listing__created_at__month
            , subq_0.listing__created_at__quarter
            , subq_0.listing__created_at__year
            , subq_0.listing__created_at__extract_year
            , subq_0.listing__created_at__extract_quarter
            , subq_0.listing__created_at__extract_month
            , subq_0.listing__created_at__extract_day
            , subq_0.listing__created_at__extract_dow
            , subq_0.listing__created_at__extract_doy
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
            , subq_0.user
            , subq_0.listing__user
            , subq_0.country_latest
            , subq_0.is_lux_latest
            , subq_0.capacity_latest
            , subq_0.listing__country_latest
            , subq_0.listing__is_lux_latest
            , subq_0.listing__capacity_latest
            , subq_0.__listings
            , subq_0.__lux_listings
            , subq_0.__smallest_listing
            , subq_0.__largest_listing
            , subq_0.__active_listings
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
        ) subq_1
      ) subq_2
    ) subq_3
    GROUP BY
      subq_3.listing__country_latest
  ) subq_4
) subq_5
