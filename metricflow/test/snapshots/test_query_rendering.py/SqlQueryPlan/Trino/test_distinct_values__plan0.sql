-- Order By ['listing__country_latest'] Limit 100
SELECT
  subq_2.listing__country_latest
FROM (
  -- Pass Only Elements:
  --   ['listing__country_latest']
  SELECT
    subq_1.listing__country_latest
  FROM (
    -- Constrain Output with WHERE
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
      , subq_0.listing
      , subq_0.user
      , subq_0.listing__user
      , subq_0.country_latest
      , subq_0.is_lux_latest
      , subq_0.capacity_latest
      , subq_0.listing__country_latest
      , subq_0.listing__is_lux_latest
      , subq_0.listing__capacity_latest
      , subq_0.listings
      , subq_0.largest_listing
      , subq_0.smallest_listing
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      SELECT
        1 AS listings
        , listings_latest_src_10005.capacity AS largest_listing
        , listings_latest_src_10005.capacity AS smallest_listing
        , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS ds__day
        , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS ds__week
        , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS ds__month
        , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS ds__quarter
        , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS ds__year
        , EXTRACT(year FROM listings_latest_src_10005.created_at) AS ds__extract_year
        , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS ds__extract_quarter
        , EXTRACT(month FROM listings_latest_src_10005.created_at) AS ds__extract_month
        , EXTRACT(day FROM listings_latest_src_10005.created_at) AS ds__extract_day
        , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_10005.created_at) AS ds__extract_dow
        , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS ds__extract_doy
        , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS created_at__day
        , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS created_at__week
        , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS created_at__month
        , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS created_at__quarter
        , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS created_at__year
        , EXTRACT(year FROM listings_latest_src_10005.created_at) AS created_at__extract_year
        , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS created_at__extract_quarter
        , EXTRACT(month FROM listings_latest_src_10005.created_at) AS created_at__extract_month
        , EXTRACT(day FROM listings_latest_src_10005.created_at) AS created_at__extract_day
        , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_10005.created_at) AS created_at__extract_dow
        , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS created_at__extract_doy
        , listings_latest_src_10005.country AS country_latest
        , listings_latest_src_10005.is_lux AS is_lux_latest
        , listings_latest_src_10005.capacity AS capacity_latest
        , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS listing__ds__day
        , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS listing__ds__week
        , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS listing__ds__month
        , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS listing__ds__quarter
        , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS listing__ds__year
        , EXTRACT(year FROM listings_latest_src_10005.created_at) AS listing__ds__extract_year
        , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS listing__ds__extract_quarter
        , EXTRACT(month FROM listings_latest_src_10005.created_at) AS listing__ds__extract_month
        , EXTRACT(day FROM listings_latest_src_10005.created_at) AS listing__ds__extract_day
        , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_10005.created_at) AS listing__ds__extract_dow
        , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS listing__ds__extract_doy
        , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS listing__created_at__day
        , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS listing__created_at__week
        , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS listing__created_at__month
        , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS listing__created_at__quarter
        , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS listing__created_at__year
        , EXTRACT(year FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_year
        , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_quarter
        , EXTRACT(month FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_month
        , EXTRACT(day FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_day
        , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_dow
        , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_doy
        , listings_latest_src_10005.country AS listing__country_latest
        , listings_latest_src_10005.is_lux AS listing__is_lux_latest
        , listings_latest_src_10005.capacity AS listing__capacity_latest
        , listings_latest_src_10005.listing_id AS listing
        , listings_latest_src_10005.user_id AS user
        , listings_latest_src_10005.user_id AS listing__user
      FROM ***************************.dim_listings_latest listings_latest_src_10005
    ) subq_0
    WHERE listing__country_latest = 'us'
  ) subq_1
  GROUP BY
    subq_1.listing__country_latest
) subq_2
ORDER BY subq_2.listing__country_latest DESC
LIMIT 100
