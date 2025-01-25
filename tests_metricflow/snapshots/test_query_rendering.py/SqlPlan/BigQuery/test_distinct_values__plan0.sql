test_name: test_distinct_values
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get distinct values for a dimension.
sql_engine: BigQuery
---
-- Order By ['listing__country_latest'] Limit 100
SELECT
  nr_subq_2.listing__country_latest
FROM (
  -- Pass Only Elements: ['listing__country_latest',]
  SELECT
    nr_subq_1.listing__country_latest
  FROM (
    -- Constrain Output with WHERE
    SELECT
      nr_subq_0.ds__day
      , nr_subq_0.ds__week
      , nr_subq_0.ds__month
      , nr_subq_0.ds__quarter
      , nr_subq_0.ds__year
      , nr_subq_0.ds__extract_year
      , nr_subq_0.ds__extract_quarter
      , nr_subq_0.ds__extract_month
      , nr_subq_0.ds__extract_day
      , nr_subq_0.ds__extract_dow
      , nr_subq_0.ds__extract_doy
      , nr_subq_0.created_at__day
      , nr_subq_0.created_at__week
      , nr_subq_0.created_at__month
      , nr_subq_0.created_at__quarter
      , nr_subq_0.created_at__year
      , nr_subq_0.created_at__extract_year
      , nr_subq_0.created_at__extract_quarter
      , nr_subq_0.created_at__extract_month
      , nr_subq_0.created_at__extract_day
      , nr_subq_0.created_at__extract_dow
      , nr_subq_0.created_at__extract_doy
      , nr_subq_0.listing__ds__day
      , nr_subq_0.listing__ds__week
      , nr_subq_0.listing__ds__month
      , nr_subq_0.listing__ds__quarter
      , nr_subq_0.listing__ds__year
      , nr_subq_0.listing__ds__extract_year
      , nr_subq_0.listing__ds__extract_quarter
      , nr_subq_0.listing__ds__extract_month
      , nr_subq_0.listing__ds__extract_day
      , nr_subq_0.listing__ds__extract_dow
      , nr_subq_0.listing__ds__extract_doy
      , nr_subq_0.listing__created_at__day
      , nr_subq_0.listing__created_at__week
      , nr_subq_0.listing__created_at__month
      , nr_subq_0.listing__created_at__quarter
      , nr_subq_0.listing__created_at__year
      , nr_subq_0.listing__created_at__extract_year
      , nr_subq_0.listing__created_at__extract_quarter
      , nr_subq_0.listing__created_at__extract_month
      , nr_subq_0.listing__created_at__extract_day
      , nr_subq_0.listing__created_at__extract_dow
      , nr_subq_0.listing__created_at__extract_doy
      , nr_subq_0.listing
      , nr_subq_0.user
      , nr_subq_0.listing__user
      , nr_subq_0.country_latest
      , nr_subq_0.is_lux_latest
      , nr_subq_0.capacity_latest
      , nr_subq_0.listing__country_latest
      , nr_subq_0.listing__is_lux_latest
      , nr_subq_0.listing__capacity_latest
      , nr_subq_0.listings
      , nr_subq_0.largest_listing
      , nr_subq_0.smallest_listing
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
    WHERE listing__country_latest = 'us'
  ) nr_subq_1
  GROUP BY
    listing__country_latest
) nr_subq_2
ORDER BY nr_subq_2.listing__country_latest DESC
LIMIT 100
