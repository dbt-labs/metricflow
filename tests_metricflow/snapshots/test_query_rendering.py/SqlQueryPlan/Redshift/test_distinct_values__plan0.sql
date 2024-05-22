-- Order By ['listing__country_latest'] Limit 100
SELECT
  subq_5.listing__country_latest
FROM (
  -- Pass Only Elements: ['listing__country_latest',]
  SELECT
    subq_4.listing__country_latest
  FROM (
    -- Constrain Output with WHERE
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
      , subq_3.created_at__day
      , subq_3.created_at__week
      , subq_3.created_at__month
      , subq_3.created_at__quarter
      , subq_3.created_at__year
      , subq_3.created_at__extract_year
      , subq_3.created_at__extract_quarter
      , subq_3.created_at__extract_month
      , subq_3.created_at__extract_day
      , subq_3.created_at__extract_dow
      , subq_3.created_at__extract_doy
      , subq_3.listing__ds__day
      , subq_3.listing__ds__week
      , subq_3.listing__ds__month
      , subq_3.listing__ds__quarter
      , subq_3.listing__ds__year
      , subq_3.listing__ds__extract_year
      , subq_3.listing__ds__extract_quarter
      , subq_3.listing__ds__extract_month
      , subq_3.listing__ds__extract_day
      , subq_3.listing__ds__extract_dow
      , subq_3.listing__ds__extract_doy
      , subq_3.listing__created_at__day
      , subq_3.listing__created_at__week
      , subq_3.listing__created_at__month
      , subq_3.listing__created_at__quarter
      , subq_3.listing__created_at__year
      , subq_3.listing__created_at__extract_year
      , subq_3.listing__created_at__extract_quarter
      , subq_3.listing__created_at__extract_month
      , subq_3.listing__created_at__extract_day
      , subq_3.listing__created_at__extract_dow
      , subq_3.listing__created_at__extract_doy
      , subq_3.listing
      , subq_3.user
      , subq_3.listing__user
      , subq_3.country_latest
      , subq_3.is_lux_latest
      , subq_3.capacity_latest
      , subq_3.listing__country_latest
      , subq_3.listing__is_lux_latest
      , subq_3.listing__capacity_latest
      , subq_3.listings
      , subq_3.largest_listing
      , subq_3.smallest_listing
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_2.ds__day
        , subq_2.ds__week
        , subq_2.ds__month
        , subq_2.ds__quarter
        , subq_2.ds__year
        , subq_2.ds__extract_year
        , subq_2.ds__extract_quarter
        , subq_2.ds__extract_month
        , subq_2.ds__extract_day
        , subq_2.ds__extract_dow
        , subq_2.ds__extract_doy
        , subq_2.created_at__day
        , subq_2.created_at__week
        , subq_2.created_at__month
        , subq_2.created_at__quarter
        , subq_2.created_at__year
        , subq_2.created_at__extract_year
        , subq_2.created_at__extract_quarter
        , subq_2.created_at__extract_month
        , subq_2.created_at__extract_day
        , subq_2.created_at__extract_dow
        , subq_2.created_at__extract_doy
        , subq_2.listing__ds__day
        , subq_2.listing__ds__week
        , subq_2.listing__ds__month
        , subq_2.listing__ds__quarter
        , subq_2.listing__ds__year
        , subq_2.listing__ds__extract_year
        , subq_2.listing__ds__extract_quarter
        , subq_2.listing__ds__extract_month
        , subq_2.listing__ds__extract_day
        , subq_2.listing__ds__extract_dow
        , subq_2.listing__ds__extract_doy
        , subq_2.listing__created_at__day
        , subq_2.listing__created_at__week
        , subq_2.listing__created_at__month
        , subq_2.listing__created_at__quarter
        , subq_2.listing__created_at__year
        , subq_2.listing__created_at__extract_year
        , subq_2.listing__created_at__extract_quarter
        , subq_2.listing__created_at__extract_month
        , subq_2.listing__created_at__extract_day
        , subq_2.listing__created_at__extract_dow
        , subq_2.listing__created_at__extract_doy
        , subq_2.listing
        , subq_2.user
        , subq_2.listing__user
        , subq_2.country_latest
        , subq_2.is_lux_latest
        , subq_2.capacity_latest
        , subq_2.listing__country_latest
        , subq_2.listing__is_lux_latest
        , subq_2.listing__capacity_latest
        , subq_2.listings
        , subq_2.largest_listing
        , subq_2.smallest_listing
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
          , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
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
          , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
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
          , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
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
          , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
          , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
          , listings_latest_src_28000.country AS listing__country_latest
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , listings_latest_src_28000.capacity AS listing__capacity_latest
          , listings_latest_src_28000.listing_id AS listing
          , listings_latest_src_28000.user_id AS user
          , listings_latest_src_28000.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_28000
      ) subq_2
      WHERE listing__country_latest = 'us'
    ) subq_3
    WHERE listing__country_latest = 'us'
  ) subq_4
  GROUP BY
    subq_4.listing__country_latest
) subq_5
ORDER BY subq_5.listing__country_latest DESC
LIMIT 100
