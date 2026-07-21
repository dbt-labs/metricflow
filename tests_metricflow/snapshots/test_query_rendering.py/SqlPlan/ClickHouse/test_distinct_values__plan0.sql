test_name: test_distinct_values
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get distinct values for a dimension.
sql_engine: ClickHouse
---
SELECT
  subq_4.listing__country_latest
FROM (
  SELECT
    subq_3.listing__country_latest
  FROM (
    SELECT
      subq_2.listing__country_latest
    FROM (
      SELECT
        subq_1.listing__country_latest
      FROM (
        SELECT
          subq_0.listing__country_latest
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
      WHERE listing__country_latest = 'us'
    ) subq_2
    GROUP BY
      subq_2.listing__country_latest
  ) subq_3
  ORDER BY subq_3.listing__country_latest DESC
  LIMIT 100
) subq_4
