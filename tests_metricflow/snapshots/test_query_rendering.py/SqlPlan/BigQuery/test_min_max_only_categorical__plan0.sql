test_name: test_min_max_only_categorical
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a categorical dimension.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_3.listing__country_latest__min
  , subq_3.listing__country_latest__max
FROM (
  -- Calculate min and max
  SELECT
    MIN(subq_2.listing__country_latest) AS listing__country_latest__min
    , MAX(subq_2.listing__country_latest) AS listing__country_latest__max
  FROM (
    -- Pass Only Elements: ['listing__country_latest']
    SELECT
      subq_1.listing__country_latest
    FROM (
      -- Pass Only Elements: ['listing__country_latest']
      SELECT
        subq_0.listing__country_latest
      FROM (
        -- Read Elements From Semantic Model 'listings_latest'
        SELECT
          1 AS __listings
          , 1 AS __lux_listings
          , listings_latest_src_28000.capacity AS __smallest_listing
          , listings_latest_src_28000.capacity AS __largest_listing
          , 1 AS __active_listings
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
      ) subq_0
    ) subq_1
    GROUP BY
      listing__country_latest
  ) subq_2
) subq_3
